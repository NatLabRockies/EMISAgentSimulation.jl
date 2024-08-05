struct RenewableFullDispatchInertia <: PSI.AbstractRenewableDispatchFormulation end
struct RenewableCleanEnergyDispatch <: PSI.AbstractRenewableDispatchFormulation end
struct RenewableEmisDispatch <: PSI.AbstractRenewableDispatchFormulation end

PSI.get_variable_binary(::ActivePowerShortageVariable, ::Type{<:PSY.RenewableGen}, _) =
    false
PSI.get_variable_lower_bound(::ActivePowerShortageVariable, d::PSY.RenewableGen, _) = 0.0
PSI.get_variable_upper_bound(::ActivePowerShortageVariable, d::PSY.RenewableGen, _) =
    PSY.get_rating(d)

function inertia_constraints!(
    optimization_container::PSI.OptimizationContainer,
    devices::IS.FlattenIteratorWrapper{T},
    model::PSI.DeviceModel{T, D},
    ::Type{S},
    feedforward::Union{Nothing, PSI.AbstractAffectFeedForward},
) where {
    T <: PSY.RenewableGen,
    D <: Union{RenewableFullDispatchInertia, RenewableEmisDispatch},
    S <: PM.AbstractPowerModel,
}
    if _has_inertia_service(model)
        service_model = _get_inertia_service_model(model)
        service = _get_services(first(devices), service_model)[1]

        time_steps = PSI.model_time_steps(optimization_container)
        use_parameters = PSI.model_has_parameters(optimization_container)
        forecast_label = "max_active_power"
        constraint_info =
            Vector{InertiaServiceRenewableConstraintInfo}(undef, length(devices))
        for (idx, d) in enumerate(devices)
            constraint_info[idx] = InertiaServiceRenewableConstraintInfo(
                PSY.get_name(d),
                _get_inertia(d),
                PSY.get_max_active_power(d),
                PSI.get_time_series(optimization_container, d, forecast_label),
            )
            # We take into account the P and upward reserve provision to
            # calculate the amount of inertia contributed.
            PSI.add_device_services!(constraint_info[idx], d, model)
        end
        if use_parameters
            parameter_ref = PSI.UpdateRef{T}(PSI.ACTIVE_POWER, forecast_label)
            renewable_device_inertia_param!(
                optimization_container,
                constraint_info,
                parameter_ref,
                PSI.make_constraint_name(INERTIA_LIMIT, T),
                PSI.make_variable_name(PSI.ACTIVE_POWER, T),
                PSI.make_variable_name(PSY.get_name(service), typeof(service)),
            )
        else
            renewable_device_inertia!(
                optimization_container,
                constraint_info,
                PSI.make_constraint_name(INERTIA_LIMIT, T),
                PSI.make_variable_name(PSI.ACTIVE_POWER, T),
                PSI.make_variable_name(PSY.get_name(service), typeof(service)),
            )
        end
    end
    return
end

function energy_contribution_constraint!(
    optimization_container::PSI.OptimizationContainer,
    devices::IS.FlattenIteratorWrapper{T},
    model::PSI.DeviceModel{T, D},
    ::Type{S},
    feedforward::Union{Nothing, PSI.AbstractAffectFeedForward},
) where {
    T <: PSY.RenewableGen,
    D <: Union{RenewableCleanEnergyDispatch, RenewableEmisDispatch},
    S <: PM.AbstractPowerModel,
}
    if _has_clean_energy_service(model)
        service_model = _get_clean_energy_service_model(model)
        service = _get_services(first(devices), service_model)[1]

        initial_time = PSI.model_initial_time(optimization_container)
        @debug initial_time
        time_steps = PSI.model_time_steps(optimization_container)
        set_name = [PSY.get_name(x) for x in devices]
        const_name = PSI.make_constraint_name(ENERGY_CONTRIBUTION_LIMIT, T)
        constraint = PSI.add_cons_container!(
            optimization_container,
            const_name,
            set_name,
            time_steps,
        )
        reserve_variable =
            PSI.get_variable(optimization_container, PSY.get_name(service), typeof(service))
        p_variable = PSI.get_variable(optimization_container, PSI.ActivePowerVariable, T)
        for t in time_steps, name in set_name
            constraint[name, t] = JuMP.@constraint(
                optimization_container.JuMPmodel,
                reserve_variable[name, t] == p_variable[name, t]
            )
        end
    end
    return
end
