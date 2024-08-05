struct InertiaFF <: PSI.AbstractAffectFeedForward
    binary_source_problem::Symbol
    affected_variables::Vector{Symbol}
    service::PSY.Reserve
    cache::Union{Nothing, Type{<:PSI.AbstractCache}}
    function InertiaFF(
        binary_source_problem::AbstractString,
        affected_variables::Vector{<:AbstractString},
        service::PSY.Reserve,
        cache::Union{Nothing, Type{<:PSI.AbstractCache}},
    )
        new(Symbol(binary_source_problem), Symbol.(affected_variables), service, cache)
    end
end

function InertiaFF(; binary_source_problem, affected_variables, service)
    return InertiaFF(binary_source_problem, affected_variables, service, nothing)
end

PSI.get_binary_source_problem(p::InertiaFF) = p.binary_source_problem

struct EnergyTargetFF <: PSI.AbstractAffectFeedForward
    variable_source_problem::Symbol
    affected_variables::Vector{Symbol}
    target_period::Int
    penalty_cost::Float64
    cache::Union{Nothing, Type{<:PSI.AbstractCache}}
    function EnergyTargetFF(
        variable_source_problem::AbstractString,
        affected_variables::Vector{<:AbstractString},
        target_period::Int,
        penalty_cost::Float64,
        cache::Union{Nothing, Type{<:PSI.AbstractCache}},
    )
        new(
            Symbol(variable_source_problem),
            Symbol.(affected_variables),
            target_period,
            penalty_cost,
            cache,
        )
    end
end

function EnergyTargetFF(;
    variable_source_problem,
    affected_variables,
    target_period,
    penalty_cost,
)
    return EnergyTargetFF(
        variable_source_problem,
        affected_variables,
        target_period,
        penalty_cost,
        nothing,
    )
end

PSI.get_variable_source_problem(p::EnergyTargetFF) = p.variable_source_problem

struct ReserveSemiContinuousFF <: PSI.AbstractAffectFeedForward
    binary_source_problem::Symbol
    affected_variables::Vector{Symbol}
    cache::Union{Nothing, Type{<:PSI.AbstractCache}}
    function ReserveSemiContinuousFF(
        binary_source_problem::AbstractString,
        affected_variables::Vector{<:AbstractString},
        cache::Union{Nothing, Type{<:PSI.AbstractCache}},
    )
        new(Symbol(binary_source_problem), Symbol.(affected_variables), cache)
    end
end

function ReserveSemiContinuousFF(; binary_source_problem, affected_variables)
    return ReserveSemiContinuousFF(binary_source_problem, affected_variables, nothing)
end

PSI.get_binary_source_problem(p::ReserveSemiContinuousFF) = p.binary_source_problem

struct EnergyCommitmentFF <: PSI.AbstractAffectFeedForward
    variable_source_problem::Symbol
    affected_variables::Vector{Symbol}
    affected_time_periods::Int
    cache::Union{Nothing, Type{<:PSI.AbstractCache}}
    function EnergyCommitmentFF(
        variable_source_problem::AbstractString,
        affected_variables::Vector{<:AbstractString},
        affected_time_periods::Int,
        cache::Union{Nothing, Type{<:PSI.AbstractCache}},
    )
        new(
            Symbol(variable_source_problem),
            Symbol.(affected_variables),
            affected_time_periods,
            cache,
        )
    end
end

function EnergyCommitmentFF(;
    variable_source_problem,
    affected_variables,
    affected_time_periods,
)
    return EnergyCommitmentFF(
        variable_source_problem,
        affected_variables,
        affected_time_periods,
        nothing,
    )
end

PSI.get_variable_source_problem(p::EnergyCommitmentFF) = p.variable_source_problem

struct EmisFF <: PSI.AbstractAffectFeedForward
    binary_source_problem::Symbol
    variable_source_problem::Symbol
    affected_variables::Vector{Symbol}
    affected_time_periods::Int
    service::PSY.Reserve
    cache::Union{Nothing, Type{<:PSI.AbstractCache}}
    function EmisFF(
        binary_source_problem::AbstractString,
        variable_source_problem::AbstractString,
        affected_variables::Vector{<:AbstractString},
        affected_time_periods::Int,
        service::PSY.Reserve,
        cache::Union{Nothing, Type{<:PSI.AbstractCache}},
    )
        new(
            Symbol(binary_source_problem),
            Symbol(variable_source_problem),
            Symbol.(affected_variables),
            affected_time_periods,
            service,
            cache,
        )
    end
end

function EmisFF(;
    binary_source_problem,
    variable_source_problem,
    affected_variables,
    affected_time_periods,
    service,
)
    return EmisFF(
        binary_source_problem,
        variable_source_problem,
        affected_variables,
        affected_time_periods,
        service,
        nothing,
    )
end

PSI.get_variable_source_problem(p::EmisFF) = p.variable_source_problem
PSI.get_binary_source_problem(p::EmisFF) = p.binary_source_problem

####################### Feed Forward Affects ###############################################
@doc raw"""

"""
function inertia_ff(
    optimization_container::PSI.OptimizationContainer,
    cons_name::Symbol,
    constraint_infos::Vector{InertiaServiceConstraintInfo},
    param_reference::PSI.UpdateRef,
    var_name::Symbol,
)
    time_steps = PSI.model_time_steps(optimization_container)
    variable = PSI.get_variable(optimization_container, var_name)

    axes = JuMP.axes(variable)
    set_name = [d.component_name for d in constraint_infos]
    @assert axes[2] == time_steps
    container = PSI.get_parameter_container(optimization_container, param_reference)
    param_on = PSI.get_parameter_array(container)
    con_ub =
        PSI.add_cons_container!(optimization_container, cons_name, set_name, time_steps)

    for d in constraint_infos
        name = PSI.get_component_name(d)
        for t in time_steps
            con_ub[name, t] = JuMP.@constraint(
                optimization_container.JuMPmodel,
                variable[name, t] <= param_on[name] * (d.h_constant * d.base_power)
            )
        end
    end
    return
end

@doc raw"""

"""
function energy_target_ff(
    optimization_container::PSI.OptimizationContainer,
    cons_name::Symbol,
    param_reference::PSI.UpdateRef,
    var_name::Tuple{Symbol, Symbol},
    target_period::Int,
    penalty_cost::Float64,
)
    time_steps = PSI.model_time_steps(optimization_container)
    variable = PSI.get_variable(optimization_container, var_name[1])
    varslack = PSI.get_variable(optimization_container, var_name[2])
    axes = JuMP.axes(variable)
    set_name = axes[1]

    @assert axes[2] == time_steps
    container_ub =
        PSI.add_param_container!(optimization_container, param_reference, set_name)
    param_ub = PSI.get_parameter_array(container_ub)
    multiplier_ub = PSI.get_multiplier_array(container_ub)
    con_ub = PSI.add_cons_container!(optimization_container, cons_name, set_name)

    for name in axes[1]
        value = JuMP.upper_bound(variable[name, 1])
        param_ub[name] = PSI.add_parameter(optimization_container.JuMPmodel, value)
        # default set to 1.0, as this implementation doesn't use multiplier
        multiplier_ub[name] = 1.0
        con_ub[name] = JuMP.@constraint(
            optimization_container.JuMPmodel,
            variable[name, target_period] + varslack[name, target_period] >=
            param_ub[name] * multiplier_ub[name]
        )
        PSI.linear_gen_cost!(
            optimization_container,
            var_name[2],
            name,
            penalty_cost,
            target_period,
        )
    end
end

function reserve_semicontinuousrange_ff(
    optimization_container::PSI.OptimizationContainer,
    cons_name::Symbol,
    constraint_infos::Vector{PSI.DeviceRangeConstraintInfo},
    param_reference::PSI.UpdateRef,
    var_name::Symbol,
)
    time_steps = PSI.model_time_steps(optimization_container)
    ub_name = PSI.middle_rename(cons_name, PSI.PSI_NAME_DELIMITER, "ub")
    lb_name = PSI.middle_rename(cons_name, PSI.PSI_NAME_DELIMITER, "lb")
    variable = PSI.get_variable(optimization_container, var_name)
    # Used to make sure the names are consistent between the variable and the infos
    axes = JuMP.axes(variable)
    set_name = [PSI.get_component_name(ci) for ci in constraint_infos]
    @assert axes[2] == time_steps
    container = PSI.add_param_container!(optimization_container, param_reference, set_name)
    multiplier = PSI.get_multiplier_array(container)
    param = PSI.get_parameter_array(container)
    con_ub = PSI.add_cons_container!(optimization_container, ub_name, set_name, time_steps)
    con_lb = PSI.add_cons_container!(optimization_container, lb_name, set_name, time_steps)

    for constraint_info in constraint_infos
        name = PSI.get_component_name(constraint_info)
        ub_value = JuMP.upper_bound(variable[name, 1])
        lb_value = JuMP.lower_bound(variable[name, 1])
        @debug "ReserveSemiContinuousFF" name ub_value lb_value
        # default set to 1.0, as this implementation doesn't use multiplier
        multiplier[name] = 1.0
        param[name] = PSI.add_parameter(optimization_container.JuMPmodel, 1.0)
        for t in time_steps
            expression_ub = JuMP.AffExpr(0.0, variable[name, t] => 1.0)
            for val in constraint_info.additional_terms_ub
                JuMP.add_to_expression!(
                    expression_ub,
                    PSI.get_variable(optimization_container, val)[name, t],
                )
            end
            expression_lb = JuMP.AffExpr(0.0, variable[name, t] => 1.0)
            for val in constraint_info.additional_terms_lb
                JuMP.add_to_expression!(
                    expression_lb,
                    PSI.get_variable(optimization_container, val)[name, t],
                    -1.0,
                )
            end
            mul_ub = ub_value * multiplier[name]
            mul_lb = lb_value * multiplier[name]
            con_ub[name, t] = JuMP.@constraint(
                optimization_container.JuMPmodel,
                expression_ub <= mul_ub * (1 - param[name])
            )
            con_lb[name, t] = JuMP.@constraint(
                optimization_container.JuMPmodel,
                expression_lb >= mul_lb * (1 - param[name])
            )
        end
    end

    # If the variable was a lower bound != 0, not removing the LB can cause infeasibilities
    for v in variable
        if JuMP.has_lower_bound(v)
            @debug "lb reset" v
            JuMP.set_lower_bound(v, 0.0)
        end
    end

    return
end

function energy_commitment_ff(
    optimization_container::PSI.OptimizationContainer,
    cons_name::Symbol,
    param_reference::PSI.UpdateRef,
    var_names::Tuple{Symbol, Symbol},
    affected_time_periods::Int,
)
    time_steps = PSI.model_time_steps(optimization_container)
    ub_name = PSI.middle_rename(cons_name, PSI.PSI_NAME_DELIMITER, "energy_commitment")
    variable = PSI.get_variable(optimization_container, var_names[1])
    varslack = PSI.get_variable(optimization_container, var_names[2])

    axes = JuMP.axes(variable)
    set_name = axes[1]

    @assert axes[2] == time_steps
    container_ub =
        PSI.add_param_container!(optimization_container, param_reference, set_name)
    param_ub = PSI.get_parameter_array(container_ub)
    multiplier_ub = PSI.get_multiplier_array(container_ub)
    con_ub = PSI.add_cons_container!(optimization_container, ub_name, set_name)

    for name in axes[1]
        value = JuMP.upper_bound(variable[name, 1])
        param_ub[name] = PSI.add_parameter(optimization_container.JuMPmodel, value)
        # default set to 1.0, as this implementation doesn't use multiplier
        multiplier_ub[name] = 1.0
        con_ub[name] = JuMP.@constraint(
            optimization_container.JuMPmodel,
            sum(variable[name, t] + varslack[name, t] for t in 1:affected_time_periods) / affected_time_periods == param_ub[name] * multiplier_ub[name]
        )
        for t in 1:affected_time_periods
            PSI.add_to_cost_expression!(
                optimization_container,
                varslack[name, t] * FEEDFORWARD_SLACK_COST,
            )
        end
    end
end

########################## FeedForward Constraints #########################################

function PSI.feedforward!(
    optimization_container::PSI.OptimizationContainer,
    devices::IS.FlattenIteratorWrapper{T},
    model::PSI.DeviceModel{T, <:PSI.AbstractDeviceFormulation},
    ff_model::InertiaFF,
) where {T <: PSY.StaticInjection}
    bin_var = PSI.make_variable_name(PSI.get_binary_source_problem(ff_model), T)
    parameter_ref = PSI.UpdateRef{JuMP.VariableRef}(bin_var)
    constraint_infos = Vector{PSI.DeviceRangeConstraintInfo}(undef, length(devices))
    inertia_constraint_infos = Vector{InertiaServiceConstraintInfo}(undef, length(devices))
    for (ix, d) in enumerate(devices)
        name = PSY.get_name(d)
        inertia = _get_inertia(d)
        device_base = PSY.get_base_power(d)
        limits = PSY.get_active_power_limits(d)
        constraint_info = PSI.DeviceRangeConstraintInfo(name, limits)
        PSI.add_device_services!(constraint_info, d, model)
        constraint_infos[ix] = constraint_info
        inertia_constraint_infos[ix] =
            InertiaServiceConstraintInfo(name, inertia, device_base)
    end
    for prefix in PSI.get_affected_variables(ff_model)
        var_name = PSI.make_variable_name(prefix, T)
        PSI.semicontinuousrange_ff(
            optimization_container,
            PSI.make_constraint_name(PSI.FEEDFORWARD_BIN, T),
            constraint_infos,
            parameter_ref,
            var_name,
        )
    end
    var_name =
        PSI.make_variable_name(PSY.get_name(ff_model.service), typeof(ff_model.service))
    inertia_ff(
        optimization_container,
        PSI.make_constraint_name(INERTIA_LIMIT, T),
        inertia_constraint_infos,
        parameter_ref,
        var_name,
    )
end

function PSI.feedforward!(
    optimization_container::PSI.OptimizationContainer,
    devices::IS.FlattenIteratorWrapper{T},
    ::PSI.DeviceModel{T, D},
    ff_model::EnergyTargetFF,
) where {T <: PSY.StaticInjection, D <: PSI.AbstractDeviceFormulation}
    PSI.add_variables!(optimization_container, PSI.EnergyShortageVariable, devices, D())
    for prefix in PSI.get_affected_variables(ff_model)
        var_name = PSI.make_variable_name(prefix, T)
        varslack_name = PSI.make_variable_name(PSI.ENERGY_SHORTAGE, T)
        source_var_name =
            PSI.make_variable_name(PSI.get_variable_source_problem(ff_model), T)
        parameter_ref = PSI.UpdateRef{JuMP.VariableRef}(source_var_name)
        energy_target_ff(
            optimization_container,
            PSI.make_constraint_name(FEEDFORWARD_ENERGY_TARGET, T),
            parameter_ref,
            (var_name, varslack_name),
            ff_model.target_period,
            ff_model.penalty_cost,
        )
    end
end

function PSI.feedforward!(
    optimization_container::PSI.OptimizationContainer,
    devices::IS.FlattenIteratorWrapper{T},
    model::PSI.DeviceModel{T, <:PSI.AbstractDeviceFormulation},
    ff_model::ReserveSemiContinuousFF,
) where {T <: PSY.StaticInjection}
    bin_var = PSI.make_variable_name(PSI.get_binary_source_problem(ff_model), T)
    parameter_ref = PSI.UpdateRef{JuMP.VariableRef}(bin_var)
    constraint_infos = Vector{PSI.DeviceRangeConstraintInfo}(undef, length(devices))
    for (ix, d) in enumerate(devices)
        name = PSY.get_name(d)
        limits = PSY.get_input_active_power_limits(d)
        constraint_info = PSI.DeviceRangeConstraintInfo(name, limits)
        PSI.add_device_services!(constraint_info, d, model)
        constraint_infos[ix] = constraint_info
    end
    for prefix in PSI.get_affected_variables(ff_model)
        var_name = PSI.make_variable_name(prefix, T)
        reserve_semicontinuousrange_ff(
            optimization_container,
            PSI.make_constraint_name(FEEDFORWARD_RESERVE_BIN, T),
            constraint_infos,
            parameter_ref,
            var_name,
        )
    end
end

function PSI.feedforward!(
    optimization_container::PSI.OptimizationContainer,
    devices::IS.FlattenIteratorWrapper{T},
    model::PSI.DeviceModel{T, D},
    ff_model::EnergyCommitmentFF,
) where {T <: PSY.StaticInjection, D <: PSI.AbstractDeviceFormulation}

    # Energy FF
    PSI.add_variables!(optimization_container, ActivePowerShortageVariable, devices, D())
    time_steps = PSI.model_time_steps(optimization_container)
    slack_var_name = PSI.make_variable_name(ActivePowerShortageVariable, T)
    slack_variable = PSI.get_variable(optimization_container, slack_var_name)
    for d in devices, t in time_steps
        name = PSY.get_name(d)
        slack_variable[name, t] = JuMP.@variable(
            optimization_container.JuMPmodel,
            base_name = "$(slack_var_name)_{$(name), $(t)}",
        )
        JuMP.set_lower_bound(slack_variable[name, t], 0.0)
    end
    for prefix in PSI.get_affected_variables(ff_model)
        var_name = PSI.make_variable_name(prefix, T)
        parameter_ref = PSI.UpdateRef{JuMP.VariableRef}(var_name)
        energy_commitment_ff(
            optimization_container,
            PSI.make_constraint_name(FEEDFORWARD_ENERGY_COMMITMENT, T),
            parameter_ref,
            (var_name, slack_var_name),
            ff_model.affected_time_periods,
        )
    end
end

function PSI.feedforward!(
    optimization_container::PSI.OptimizationContainer,
    devices::IS.FlattenIteratorWrapper{T},
    model::PSI.DeviceModel{T, D},
    ff_model::EmisFF,
) where {T <: PSY.StaticInjection, D <: PSI.AbstractDeviceFormulation}
    bin_var = PSI.make_variable_name(PSI.get_binary_source_problem(ff_model), T)
    parameter_ref = PSI.UpdateRef{JuMP.VariableRef}(bin_var)
    constraint_infos = Vector{PSI.DeviceRangeConstraintInfo}(undef, length(devices))
    inertia_constraint_infos = Vector{InertiaServiceConstraintInfo}(undef, length(devices))
    for (ix, d) in enumerate(devices)
        name = PSY.get_name(d)
        inertia = _get_inertia(d)
        device_base = PSY.get_base_power(d)
        limits = PSY.get_active_power_limits(d)
        constraint_info = PSI.DeviceRangeConstraintInfo(name, limits)
        PSI.add_device_services!(constraint_info, d, model)
        constraint_infos[ix] = constraint_info
        inertia_constraint_infos[ix] =
            InertiaServiceConstraintInfo(name, inertia, device_base)
    end
    for prefix in PSI.get_affected_variables(ff_model)
        # Semi-Continuous FF
        var_name = PSI.make_variable_name(prefix, T)
        PSI.semicontinuousrange_ff(
            optimization_container,
            PSI.make_constraint_name(PSI.FEEDFORWARD_BIN, T),
            constraint_infos,
            parameter_ref,
            var_name,
        )
    end
    # Inertia FF
    var_name =
        PSI.make_variable_name(PSY.get_name(ff_model.service), typeof(ff_model.service))
    inertia_ff(
        optimization_container,
        PSI.make_constraint_name(INERTIA_LIMIT, T),
        inertia_constraint_infos,
        parameter_ref,
        var_name,
    )

    # Energy FF
    PSI.add_variables!(optimization_container, ActivePowerShortageVariable, devices, D())
    time_steps = PSI.model_time_steps(optimization_container)
    slack_var_name = PSI.make_variable_name(ActivePowerShortageVariable, T)
    slack_variable = PSI.get_variable(optimization_container, slack_var_name)
    for d in devices, t in time_steps
        name = PSY.get_name(d)
        slack_variable[name, t] = JuMP.@variable(
            optimization_container.JuMPmodel,
            base_name = "$(slack_var_name)_{$(name), $(t)}",
        )
        JuMP.set_lower_bound(slack_variable[name, t], 0.0)
    end
    for prefix in PSI.get_affected_variables(ff_model)
        var_name = PSI.make_variable_name(prefix, T)
        parameter_ref = PSI.UpdateRef{JuMP.VariableRef}(var_name)
        energy_commitment_ff(
            optimization_container,
            PSI.make_constraint_name(FEEDFORWARD_ENERGY_COMMITMENT, T),
            parameter_ref,
            (var_name, slack_var_name),
            ff_model.affected_time_periods,
        )
    end
end
