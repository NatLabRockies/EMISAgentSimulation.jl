function renewable_device_inertia!(
    optimization_container::PSI.OptimizationContainer,
    data::Vector{InertiaServiceRenewableConstraintInfo},
    cons_name::Symbol,
    var_name::Symbol,
    service_name::Symbol,
)
    time_steps = PSI.model_time_steps(optimization_container)
    var = PSI.get_variable(optimization_container, var_name)
    var_service = PSI.get_variable(optimization_container, service_name)
    var_names = [d.component_name for d in data]
    constraint =
        PSI.add_cons_container!(optimization_container, cons_name, var_names, time_steps)

    for t in time_steps, d in data
        name = PSI.get_component_name(d)
        expression_ub = JuMP.AffExpr(0.0, var[name, t] => 1.0)
        for val in d.additional_terms_ub
            JuMP.add_to_expression!(
                expression_ub,
                PSI.get_variable(optimization_container, val)[name, t],
            )
        end
        constraint[name, t] = JuMP.@constraint(
            optimization_container.JuMPmodel,
            var_service[name, t] ==
            d.h_constant * (d.timeseries[t] * d.multiplier - expression_ub)
        )
    end
    return
end

function renewable_device_inertia_param!(
    optimization_container::PSI.OptimizationContainer,
    data::Vector{InertiaServiceRenewableConstraintInfo},
    paramerter_ref::PSI.UpdateRef,
    cons_name::Symbol,
    var_name::Symbol,
    service_name::Symbol,
)
    time_steps = PSI.model_time_steps(optimization_container)
    var = PSI.get_variable(optimization_container, var_name)
    var_service = PSI.get_variable(optimization_container, service_name)
    varstart_names = [d.component_name for d in data]
    constraint = PSI.add_cons_container!(
        optimization_container,
        cons_name,
        varstart_names,
        time_steps,
    )
    container = PSI.get_parameter_container(optimization_container, paramerter_ref)
    param_ub = PSI.get_parameter_array(container)
    multiplier = PSI.get_multiplier_array(container)

    for t in time_steps, d in data
        name = PSI.get_component_name(d)
        expression_ub = JuMP.AffExpr(0.0, var[name, t] => 1.0)
        for val in d.additional_terms_ub
            JuMP.add_to_expression!(
                expression_ub,
                PSI.get_variable(optimization_container, val)[name, t],
            )
        end
        constraint[name, t] = JuMP.@constraint(
            optimization_container.JuMPmodel,
            var_service[name, t] ==
            d.h_constant * (param_ub[name, t] * multiplier[name, t] - expression_ub)
        )
    end
    return
end

function storage_device_inertia!(
    optimization_container::PSI.OptimizationContainer,
    data::Vector{InertiaServiceConstraintInfo},
    cons_name::Tuple{Symbol, Symbol},
    var_names::Tuple{Symbol, Symbol},
    service_name::Symbol,
)
    time_steps = PSI.model_time_steps(optimization_container)
    var_p = PSI.get_variable(optimization_container, var_names[1])
    var_e = PSI.get_variable(optimization_container, var_names[2])
    var_service = PSI.get_variable(optimization_container, service_name)
    var_names = [d.component_name for d in data]
    power_bound =
        PSI.add_cons_container!(optimization_container, cons_name[1], var_names, time_steps)
    energy_bound =
        PSI.add_cons_container!(optimization_container, cons_name[2], var_names, time_steps)

    for t in time_steps, d in data
        name = PSI.get_component_name(d)
        expression_ub = JuMP.AffExpr(0.0, var_p[name, t] => 1.0)
        for val in d.additional_terms_ub
            JuMP.add_to_expression!(
                expression_ub,
                PSI.get_variable(optimization_container, val)[name, t],
            )
        end

        power_bound[name, t] = JuMP.@constraint(
            optimization_container.JuMPmodel,
            var_service[name, t] == d.h_constant * (d.base_power - expression_ub)
        )

        energy_bound[name, t] = JuMP.@constraint(
            optimization_container.JuMPmodel,
            var_service[name, t] <= var_e[name, t] * PSI.SECONDS_IN_HOUR
        )
    end
    return
end

function hydro_device_inertia!(
    optimization_container::PSI.OptimizationContainer,
    data::Vector{InertiaCommitmentConstraintInfo},
    cons_name::Symbol,
    var_name::Symbol,
    service_name::Symbol,
)
    time_steps = PSI.model_time_steps(optimization_container)
    var = PSI.get_variable(optimization_container, var_name)
    var_service = PSI.get_variable(optimization_container, service_name)
    var_names = [d.component_name for d in data]
    constraint =
        PSI.add_cons_container!(optimization_container, cons_name, var_names, time_steps)

    for t in time_steps, d in data
        name = PSI.get_component_name(d)
        constraint[name, t] = JuMP.@constraint(
            optimization_container.JuMPmodel,
            var_service[name, t] == var[name, t] * d.h_constant * d.base_power
        )
    end
    return
end
