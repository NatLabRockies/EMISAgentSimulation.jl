function device_commitment_inertia!(
    optimization_container::PSI.OptimizationContainer,
    data::Vector{InertiaCommitmentConstraintInfo},
    cons_name::Symbol,
    var_names::Tuple{Symbol, Symbol, Symbol},
    service_name::Symbol,
)
    time_steps = PSI.model_time_steps(optimization_container)
    varstart = PSI.get_variable(optimization_container, var_names[1])
    varstop = PSI.get_variable(optimization_container, var_names[2])
    varon = PSI.get_variable(optimization_container, var_names[3])
    var_service = PSI.get_variable(optimization_container, service_name)
    varstart_names = axes(varstart, 1)
    constraint = PSI.add_cons_container!(
        optimization_container,
        cons_name,
        varstart_names,
        time_steps,
    )
    aux_cons_name = PSI.middle_rename(cons_name, PSI.PSI_NAME_DELIMITER, "aux")
    aux_constraint = PSI.add_cons_container!(
        optimization_container,
        aux_cons_name,
        varstart_names,
        time_steps,
    )
    service_cons_name = PSI.middle_rename(cons_name, PSI.PSI_NAME_DELIMITER, "service")
    service_constraint = PSI.add_cons_container!(
        optimization_container,
        service_cons_name,
        varstart_names,
        time_steps,
    )

    for d in data
        name = PSY.get_name(d.ic_power.device)
        constraint[name, 1] = JuMP.@constraint(
            optimization_container.JuMPmodel,
            varon[name, 1] == d.ic_power.value + varstart[name, 1] - varstop[name, 1]
        )
        aux_constraint[name, 1] = JuMP.@constraint(
            optimization_container.JuMPmodel,
            varstart[name, 1] + varstop[name, 1] <= 1.0
        )
        service_constraint[name, 1] = JuMP.@constraint(
            optimization_container.JuMPmodel,
            var_service[name, 1] == varon[name, 1] * (d.h_constant * d.base_power)
        )
    end

    for t in time_steps[2:end], d in data
        name = PSY.get_name(d.ic_power.device)
        constraint[name, t] = JuMP.@constraint(
            optimization_container.JuMPmodel,
            varon[name, t] == varon[name, t - 1] + varstart[name, t] - varstop[name, t]
        )
        aux_constraint[name, t] = JuMP.@constraint(
            optimization_container.JuMPmodel,
            varstart[name, t] + varstop[name, t] <= 1.0
        )
        service_constraint[name, t] = JuMP.@constraint(
            optimization_container.JuMPmodel,
            var_service[name, t] == varon[name, t] * (d.h_constant * d.base_power)
        )
    end
    return
end
