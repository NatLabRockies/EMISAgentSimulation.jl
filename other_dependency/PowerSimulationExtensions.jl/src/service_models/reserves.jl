struct QuadraticCostRampReserve <: PSI.AbstractReservesFormulation end
struct VariableInertiaReserve <: PSI.AbstractReservesFormulation end
struct EnergyRequirementReserve <: PSI.AbstractReservesFormulation end

PSI.get_variable_binary(
    ::InertiaServiceVariable,
    ::Type{<:PSY.Reserve},
    ::PSI.AbstractReservesFormulation,
) = false
PSI.get_variable_upper_bound(
    ::InertiaServiceVariable,
    ::PSY.Reserve,
    d::PSY.ThermalGen,
    _,
) = _get_inertia(d) * PSY.get_base_power(d)
PSI.get_variable_upper_bound(
    ::InertiaServiceVariable,
    ::PSY.Reserve,
    d::PSY.RenewableGen,
    _,
) = _get_inertia(d) * PSY.get_rating(d)
PSI.get_variable_upper_bound(::InertiaServiceVariable, ::PSY.Reserve, d::PSY.Storage, _) =
    _get_inertia(d) * PSY.get_rating(d)
PSI.get_variable_upper_bound(::InertiaServiceVariable, ::PSY.Reserve, d::PSY.HydroGen, _) =
    _get_inertia(d) * PSY.get_base_power(d)
PSI.get_variable_lower_bound(::InertiaServiceVariable, ::PSY.Reserve, ::PSY.Component, _) =
    0.0

function _get_inertia(d::PSY.Component)
    if haskey(PSY.get_ext(d), "inertia")
        return PSY.get_ext(d)["inertia"]
    else
        @info(
            "Inertia data (h-constant) is missing in Generator $(PSY.get_name(d)), h-constant will be set to 0."
        )
        return 0.0
    end
end

_get_ramp_limits(::PSY.Component) = nothing
_get_ramp_limits(d::PSY.ThermalGen) = PSY.get_ramp_limits(d)
_get_ramp_limits(d::PSY.HydroGen) = PSY.get_ramp_limits(d)

function _get_data_for_ramp_limit(
    optimization_container::PSI.OptimizationContainer,
    service::SR,
    contributing_devices::U,
) where {
    SR <: PSY.Reserve,
    U <: Union{Vector{D}, IS.FlattenIteratorWrapper{D}},
} where {D <: PSY.Component}
    time_frame = PSY.get_time_frame(service)
    resolution = PSI.model_resolution(optimization_container)
    if resolution > Dates.Minute(1)
        minutes_per_period = Dates.value(Dates.Minute(resolution))
    else
        @warn("Not all formulations support under 1-minute resolutions. Exercise caution.")
        minutes_per_period = Dates.value(Dates.Second(resolution)) / 60
    end
    lenght_contributing_devices = length(contributing_devices)
    idx = 0
    data = Vector{ServiceRampConstraintInfo}(undef, lenght_contributing_devices)

    for d in contributing_devices
        name = PSY.get_name(d)
        non_binding_up = false
        non_binding_down = false
        ramp_limits = _get_ramp_limits(d)
        if !(ramp_limits === nothing)
            p_lims = PSY.get_active_power_limits(d)
            max_rate = abs(p_lims.min - p_lims.max) / time_frame
            if (ramp_limits.up >= max_rate) & (ramp_limits.down >= max_rate)
                @debug "Generator $(name) has a nonbinding ramp limits. Constraints Skipped"
                continue
            else
                idx += 1
            end
            ramp = (up = ramp_limits.up * time_frame, down = ramp_limits.down * time_frame)
            data[idx] = ServiceRampConstraintInfo(name, ramp)
        end
    end
    if idx < lenght_contributing_devices
        deleteat!(data, (idx + 1):lenght_contributing_devices)
    end
    return data
end

function PSI.service_requirement_constraint!(
    optimization_container::PSI.OptimizationContainer,
    service::SR,
    ::PSI.ServiceModel{SR, EnergyRequirementReserve},
) where {SR <: PSY.Reserve}
    parameters = PSI.model_has_parameters(optimization_container)
    use_forecast_data = PSI.model_uses_forecasts(optimization_container)
    initial_time = PSI.model_initial_time(optimization_container)

    time_steps = PSI.model_time_steps(optimization_container)
    name = PSY.get_name(service)
    constraint = PSI.get_constraint(
        optimization_container,
        PSI.make_constraint_name(PSI.REQUIREMENT, SR),
    )
    reserve_variable = PSI.get_variable(optimization_container, name, SR)
    use_slacks = PSI.get_services_slack_variables(optimization_container.settings)
    ts_vector = PSI.get_time_series(optimization_container, service, "requirement")
    use_slacks && (slack_vars = PSI.reserve_slacks(optimization_container, name))
    requirement = PSY.get_requirement(service)

    if parameters
        container = PSI.get_parameter_container(
            optimization_container,
            PSI.UpdateRef{SR}(PSI.SERVICE_REQUIREMENT, "requirement"),
        )
        param = PSI.get_parameter_array(container)
        multiplier = PSI.get_multiplier_array(container)
        for t in time_steps
            param[name, t] =
                PSI.add_parameter(optimization_container.JuMPmodel, ts_vector[t])
            multiplier[name, t] = 1.0
        end
        if use_slacks
            resource_expression =
                sum(sum(reserve_variable[:, t]) + slack_vars[t] for t in time_steps)
        else
            resource_expression = sum(sum(reserve_variable[:, t]) for t in time_steps)
        end
        constraint[name] = JuMP.@constraint(
            optimization_container.JuMPmodel,
            resource_expression >= sum(param[name, t] * requirement for t in time_steps)
        )
    else
        constraint[name] = JuMP.@constraint(
            optimization_container.JuMPmodel,
            sum(sum(reserve_variable[:, t]) for t in time_steps) >=
            sum(ts_vector[t] * requirement for t in time_steps)
        )
    end
    return
end

function PSI.service_requirement_constraint!(
    optimization_container::PSI.OptimizationContainer,
    service::SR,
    ::PSI.ServiceModel{SR, VariableInertiaReserve},
) where {SR <: PSY.Reserve}
    parameters = PSI.model_has_parameters(optimization_container)
    use_forecast_data = PSI.model_uses_forecasts(optimization_container)
    initial_time = PSI.model_initial_time(optimization_container)
    @debug initial_time
    time_steps = PSI.model_time_steps(optimization_container)
    name = PSY.get_name(service)
    constraint = PSI.get_constraint(
        optimization_container,
        PSI.make_constraint_name(PSI.REQUIREMENT, SR),
    )
    reserve_variable = PSI.get_variable(optimization_container, name, SR)
    use_slacks = PSI.get_services_slack_variables(optimization_container.settings)

    ts_vector = PSI.get_time_series(optimization_container, service, "requirement")

    use_slacks && (slack_vars = PSI.reserve_slacks(optimization_container, name))

    requirement = PSY.get_requirement(service)
    if parameters
        container = PSI.get_parameter_container(
            optimization_container,
            PSI.UpdateRef{SR}(PSI.SERVICE_REQUIREMENT, "requirement"),
        )
        param = PSI.get_parameter_array(container)
        multiplier = PSI.get_multiplier_array(container)
        for t in time_steps
            param[name, t] =
                PSI.add_parameter(optimization_container.JuMPmodel, ts_vector[t])
            multiplier[name, t] = 1.0
            if use_slacks
                resource_expression = sum(reserve_variable[:, t]) + slack_vars[t]
            else
                resource_expression = sum(reserve_variable[:, t])
            end
            mul = (requirement * multiplier[name, t])
            constraint[name, t] = JuMP.@constraint(
                optimization_container.JuMPmodel,
                resource_expression >= param[name, t] * mul
            )
        end
    else
        for t in time_steps
            constraint[name, t] = JuMP.@constraint(
                optimization_container.JuMPmodel,
                sum(reserve_variable[:, t]) >= ts_vector[t] * requirement
            )
        end
    end
    return
end

function PSI.service_requirement_constraint!(
    optimization_container::PSI.OptimizationContainer,
    service::SR,
    ::PSI.ServiceModel{SR, QuadraticCostRampReserve},
) where {SR <: PSY.ReserveDemandCurve}
    initial_time = PSI.model_initial_time(optimization_container)
    @debug initial_time
    time_steps = PSI.model_time_steps(optimization_container)
    name = PSY.get_name(service)
    constraint = PSI.get_constraint(
        optimization_container,
        PSI.make_constraint_name(PSI.REQUIREMENT, SR),
    )
    reserve_variable = PSI.get_variable(optimization_container, name, SR)
    requirement_variable =
        PSI.get_variable(optimization_container, PSI.SERVICE_REQUIREMENT, SR)

    for t in time_steps
        constraint[name, t] = JuMP.@constraint(
            optimization_container.JuMPmodel,
            sum(reserve_variable[:, t]) == requirement_variable[name, t]
        )
    end

    return
end

function PSI.modify_device_model!(
    devices_template::Dict{Symbol, PSI.DeviceModel},
    service_model::PSI.ServiceModel{<:PSY.Reserve, VariableInertiaReserve},
    contributing_devices::Vector{<:PSY.Device},
)
    device_types = unique(typeof.(contributing_devices))
    for dt in device_types
        for (device_model_name, device_model) in devices_template
            PSI.get_component_type(device_model) != dt && continue
            service_model in device_model.services && continue
            push!(device_model.services, service_model)
        end
    end

    return
end

function PSI.include_service!(
    constraint_info::T,
    services,
    ::PSI.ServiceModel{SR, QuadraticCostRampReserve},
) where {T <: PSI.AbstractRampConstraintInfo, SR <: PSY.Reserve{PSY.ReserveUp}}
    return
end

function PSI.include_service!(
    constraint_info::T,
    services,
    ::PSI.ServiceModel{SR, QuadraticCostRampReserve},
) where {T <: PSI.AbstractRampConstraintInfo, SR <: PSY.Reserve{PSY.ReserveDown}}
    return
end

function PSI.include_service!(
    constraint_info::T,
    services,
    ::PSI.ServiceModel{SR, VariableInertiaReserve},
) where {
    T <: Union{PSI.AbstractRangeConstraintInfo, PSI.AbstractRampConstraintInfo},
    SR <: PSY.Reserve,
}
    return
end

function PSI.modify_device_model!(
    devices_template::Dict{Symbol, PSI.DeviceModel},
    service_model::PSI.ServiceModel{<:PSY.Reserve, EnergyRequirementReserve},
    contributing_devices::Vector{<:PSY.Device},
)
    device_types = unique(typeof.(contributing_devices))
    for dt in device_types
        for (device_model_name, device_model) in devices_template
            PSI.get_component_type(device_model) != dt && continue
            service_model in device_model.services && continue
            push!(device_model.services, service_model)
        end
    end

    return
end

function PSI.include_service!(
    constraint_info::T,
    services,
    ::PSI.ServiceModel{SR, EnergyRequirementReserve},
) where {
    T <: Union{PSI.AbstractRangeConstraintInfo, PSI.AbstractRampConstraintInfo},
    SR <: PSY.Reserve,
}
    return
end

function PSI.AddCostSpec(
    ::Type{T},
    ::Type{QuadraticCostRampReserve},
    optimization_container::PSI.OptimizationContainer,
) where {T <: PSY.Reserve}
    return PSI.AddCostSpec(;
        variable_type = PSI.ServiceRequirementVariable,
        component_type = T,
        has_status_variable = false,
        has_status_parameter = false,
        variable_cost = PSY.get_variable,
        start_up_cost = nothing,
        shut_down_cost = nothing,
        fixed_cost = nothing,
        sos_status = PSI.SOSStatusVariable.NO_VARIABLE,
        multiplier = PSI.OBJECTIVE_FUNCTION_NEGATIVE,
    )
end

function add_quadratic_cost!(
    optimization_container::PSI.OptimizationContainer,
    spec::PSI.AddCostSpec,
    service::SR,
    component_name::String,
) where {SR <: PSY.Reserve}
    time_steps = PSI.model_time_steps(optimization_container)
    use_forecast_data = PSI.model_uses_forecasts(optimization_container)
    if !use_forecast_data
        error("QuadraticCostRampReserve is only supported with forecast")
    end
    variable_cost_forecast =
        PSI.get_time_series(optimization_container, service, "variable_cost")
    variable_cost_forecast = map(PSY.VariableCost, variable_cost_forecast)
    for t in time_steps
        quadratic_cost!(
            optimization_container,
            spec,
            component_name,
            variable_cost_forecast[t],
            t,
        )
    end
    return
end

function PSI.cost_function!(
    optimization_container::PSI.OptimizationContainer,
    service::SR,
    model::PSI.ServiceModel{SR, QuadraticCostRampReserve},
) where {SR <: PSY.ReserveDemandCurve}
    spec = PSI.AddCostSpec(SR, model.formulation, optimization_container)
    @debug SR, spec
    add_quadratic_cost!(optimization_container, spec, service, PSY.get_name(service))
    return
end
