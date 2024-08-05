"""
Construct model for HydroGen with RunOfRiver Commitment Formulation
with only Active Power.
"""
function PSI.construct_device!(
    optimization_container::PSI.OptimizationContainer,
    sys::PSY.System,
    model::PSI.DeviceModel{H, D},
    ::Type{S},
) where {
    H <: PSY.HydroGen,
    D <: HydroInertiaCommitmentRunOfRiver,
    S <: PM.AbstractPowerModel,
}
    devices = PSI.get_available_components(H, sys)

    if !PSI.validate_available_devices(H, devices)
        return
    end

    # Variables
    PSI.add_variables!(optimization_container, PSI.ActivePowerVariable, devices, D())
    PSI.add_variables!(optimization_container, PSI.ReactivePowerVariable, devices, D())
    PSI.add_variables!(optimization_container, PSI.OnVariable, devices, D())

    # Constraints
    PSI.add_constraints!(
        optimization_container,
        PSI.RangeConstraint,
        PSI.ActivePowerVariable,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.add_constraints!(
        optimization_container,
        PSI.RangeConstraint,
        PSI.ReactivePowerVariable,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.commit_hydro_active_power_ub!(
        optimization_container,
        devices,
        model,
        PSI.get_feedforward(model),
    )
    inertia_constraints!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.feedforward!(optimization_container, devices, model, PSI.get_feedforward(model))

    # Cost Function
    PSI.cost_function!(optimization_container, devices, model, S, nothing)

    return
end

function PSI.construct_device!(
    optimization_container::PSI.OptimizationContainer,
    sys::PSY.System,
    model::PSI.DeviceModel{H, D},
    ::Type{S},
) where {
    H <: PSY.HydroGen,
    D <: HydroInertiaCommitmentRunOfRiver,
    S <: PM.AbstractActivePowerModel,
}
    devices = PSI.get_available_components(H, sys)

    if !PSI.validate_available_devices(H, devices)
        return
    end

    # Variables
    PSI.add_variables!(optimization_container, PSI.ActivePowerVariable, devices, D())
    PSI.add_variables!(optimization_container, PSI.OnVariable, devices, D())

    # Constraints
    PSI.add_constraints!(
        optimization_container,
        PSI.RangeConstraint,
        PSI.ActivePowerVariable,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.commit_hydro_active_power_ub!(
        optimization_container,
        devices,
        model,
        PSI.get_feedforward(model),
    )
    inertia_constraints!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.feedforward!(optimization_container, devices, model, PSI.get_feedforward(model))

    # Cost Function
    PSI.cost_function!(optimization_container, devices, model, S, nothing)

    return
end

function PSI.construct_device!(
    optimization_container::PSI.OptimizationContainer,
    sys::PSY.System,
    model::PSI.DeviceModel{H, D},
    ::Type{S},
) where {H <: PSY.HydroGen, D <: HydroCleanEnergyRunOfRiver, S <: PM.AbstractPowerModel}
    devices = PSI.get_available_components(H, sys)

    if !PSI.validate_available_devices(H, devices)
        return
    end

    # Variables
    PSI.add_variables!(optimization_container, PSI.ActivePowerVariable, devices, D())
    PSI.add_variables!(optimization_container, PSI.ReactivePowerVariable, devices, D())
    PSI.add_variables!(optimization_container, PSI.OnVariable, devices, D())

    # Constraints
    PSI.add_constraints!(
        optimization_container,
        PSI.RangeConstraint,
        PSI.ActivePowerVariable,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.add_constraints!(
        optimization_container,
        PSI.RangeConstraint,
        PSI.ReactivePowerVariable,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    energy_contribution_constraint!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.feedforward!(optimization_container, devices, model, PSI.get_feedforward(model))

    # Cost Function
    PSI.cost_function!(optimization_container, devices, model, S, nothing)

    return
end

function PSI.construct_device!(
    optimization_container::PSI.OptimizationContainer,
    sys::PSY.System,
    model::PSI.DeviceModel{H, D},
    ::Type{S},
) where {
    H <: PSY.HydroGen,
    D <: HydroCleanEnergyRunOfRiver,
    S <: PM.AbstractActivePowerModel,
}
    devices = PSI.get_available_components(H, sys)

    if !PSI.validate_available_devices(H, devices)
        return
    end

    # Variables
    PSI.add_variables!(optimization_container, PSI.ActivePowerVariable, devices, D())
    PSI.add_variables!(optimization_container, PSI.OnVariable, devices, D())

    # Constraints
    PSI.add_constraints!(
        optimization_container,
        PSI.RangeConstraint,
        PSI.ActivePowerVariable,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.commit_hydro_active_power_ub!(
        optimization_container,
        devices,
        model,
        PSI.get_feedforward(model),
    )
    energy_contribution_constraint!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.feedforward!(optimization_container, devices, model, PSI.get_feedforward(model))

    # Cost Function
    PSI.cost_function!(optimization_container, devices, model, S, nothing)

    return
end

function PSI.construct_device!(
    optimization_container::PSI.OptimizationContainer,
    sys::PSY.System,
    model::PSI.DeviceModel{H, D},
    ::Type{S},
) where {H <: PSY.HydroGen, D <: HydroEmisCommitmentRunOfRiver, S <: PM.AbstractPowerModel}
    devices = PSI.get_available_components(H, sys)

    if !PSI.validate_available_devices(H, devices)
        return
    end

    # Variables
    PSI.add_variables!(optimization_container, PSI.ActivePowerVariable, devices, D())
    PSI.add_variables!(optimization_container, PSI.ReactivePowerVariable, devices, D())
    PSI.add_variables!(optimization_container, PSI.OnVariable, devices, D())

    # Constraints
    PSI.add_constraints!(
        optimization_container,
        PSI.RangeConstraint,
        PSI.ActivePowerVariable,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.add_constraints!(
        optimization_container,
        PSI.RangeConstraint,
        PSI.ReactivePowerVariable,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.commit_hydro_active_power_ub!(
        optimization_container,
        devices,
        model,
        PSI.get_feedforward(model),
    )
    inertia_constraints!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    energy_contribution_constraint!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.feedforward!(optimization_container, devices, model, PSI.get_feedforward(model))

    # Cost Function
    PSI.cost_function!(optimization_container, devices, model, S, nothing)

    return
end

function PSI.construct_device!(
    optimization_container::PSI.OptimizationContainer,
    sys::PSY.System,
    model::PSI.DeviceModel{H, D},
    ::Type{S},
) where {
    H <: PSY.HydroGen,
    D <: HydroEmisCommitmentRunOfRiver,
    S <: PM.AbstractActivePowerModel,
}
    devices = PSI.get_available_components(H, sys)

    if !PSI.validate_available_devices(H, devices)
        return
    end

    # Variables
    PSI.add_variables!(optimization_container, PSI.ActivePowerVariable, devices, D())
    PSI.add_variables!(optimization_container, PSI.OnVariable, devices, D())

    # Constraints
    PSI.add_constraints!(
        optimization_container,
        PSI.RangeConstraint,
        PSI.ActivePowerVariable,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.commit_hydro_active_power_ub!(
        optimization_container,
        devices,
        model,
        PSI.get_feedforward(model),
    )
    inertia_constraints!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    energy_contribution_constraint!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.feedforward!(optimization_container, devices, model, PSI.get_feedforward(model))

    # Cost Function
    PSI.cost_function!(optimization_container, devices, model, S, nothing)

    return
end
