function service_upward_rateofchange!(
    optimization_container::PSI.OptimizationContainer,
    rate_data::Vector{ServiceRampConstraintInfo},
    cons_name::Symbol,
    var_name::Symbol,
    service_name::AbstractString,
)
    time_steps = PSI.model_time_steps(optimization_container)
    up_name = PSI.middle_rename(cons_name, PSI.PSI_NAME_DELIMITER, "up" * service_name)

    variable = PSI.get_variable(optimization_container, var_name)

    set_name = [PSI.get_component_name(r) for r in rate_data]
    con_up = PSI.add_cons_container!(optimization_container, up_name, set_name, time_steps)

    for r in rate_data, t in time_steps
        name = PSI.get_component_name(r)
        con_up[name, t] = JuMP.@constraint(
            optimization_container.JuMPmodel,
            variable[name, t] <= r.ramp_limits.up
        )
    end

    return
end

function service_downward_rateofchange!(
    optimization_container::PSI.OptimizationContainer,
    rate_data::Vector{ServiceRampConstraintInfo},
    cons_name::Symbol,
    var_name::Symbol,
    service_name::AbstractString,
)
    time_steps = PSI.model_time_steps(optimization_container)
    down_name = PSI.middle_rename(cons_name, PSI.PSI_NAME_DELIMITER, "dn" * service_name)

    variable = PSI.get_variable(optimization_container, var_name)

    set_name = [PSI.get_component_name(r) for r in rate_data]
    con_down =
        PSI.add_cons_container!(optimization_container, down_name, set_name, time_steps)

    for r in rate_data, t in time_steps
        name = PSI.get_component_name(r)
        con_down[name, t] = JuMP.@constraint(
            optimization_container.JuMPmodel,
            variable[name, t] <= r.ramp_limits.down
        )
    end

    return
end
