
function PSI.construct_device!(
    optimization_container::PSI.OptimizationContainer,
    sys::PSY.System,
    model::PSI.DeviceModel{R, RenewableFullDispatchInertia},
    ::Type{S},
) where {R <: PSY.RenewableGen, S <: PM.AbstractPowerModel}
    devices = PSI.get_available_components(R, sys)

    if !PSI.validate_available_devices(R, devices)
        return
    end

    # Variables
    PSI.add_variables!(
        optimization_container,
        PSI.ActivePowerVariable,
        devices,
        RenewableFullDispatchInertia(),
    )
    PSI.add_variables!(
        optimization_container,
        PSI.ReactivePowerVariable,
        devices,
        RenewableFullDispatchInertia(),
    )

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
    inertia_constraints!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.feedforward!(optimization_container, devices, model, PSI.get_feedforward(model))

    # Cost Function
    PSI.cost_function!(optimization_container, devices, model, S)

    return
end

function PSI.construct_device!(
    optimization_container::PSI.OptimizationContainer,
    sys::PSY.System,
    model::PSI.DeviceModel{R, RenewableFullDispatchInertia},
    ::Type{S},
) where {R <: PSY.RenewableGen, S <: PM.AbstractActivePowerModel}
    devices = PSI.get_available_components(R, sys)

    if !PSI.validate_available_devices(R, devices)
        return
    end

    # Variables
    PSI.add_variables!(
        optimization_container,
        PSI.ActivePowerVariable,
        devices,
        RenewableFullDispatchInertia(),
    )

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
    inertia_constraints!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.feedforward!(optimization_container, devices, model, PSI.get_feedforward(model))

    # Cost Function
    PSI.cost_function!(optimization_container, devices, model, S)

    return
end

function PSI.construct_device!(
    optimization_container::PSI.OptimizationContainer,
    sys::PSY.System,
    model::PSI.DeviceModel{R, RenewableCleanEnergyDispatch},
    ::Type{S},
) where {R <: PSY.RenewableGen, S <: PM.AbstractPowerModel}
    devices = PSI.get_available_components(R, sys)

    if !PSI.validate_available_devices(R, devices)
        return
    end

    # Variables
    PSI.add_variables!(
        optimization_container,
        PSI.ActivePowerVariable,
        devices,
        RenewableCleanEnergyDispatch(),
    )
    PSI.add_variables!(
        optimization_container,
        PSI.ReactivePowerVariable,
        devices,
        RenewableCleanEnergyDispatch(),
    )

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
    PSI.cost_function!(optimization_container, devices, model, S)

    return
end

function PSI.construct_device!(
    optimization_container::PSI.OptimizationContainer,
    sys::PSY.System,
    model::PSI.DeviceModel{R, RenewableCleanEnergyDispatch},
    ::Type{S},
) where {R <: PSY.RenewableGen, S <: PM.AbstractActivePowerModel}
    devices = PSI.get_available_components(R, sys)

    if !PSI.validate_available_devices(R, devices)
        return
    end

    # Variables
    PSI.add_variables!(
        optimization_container,
        PSI.ActivePowerVariable,
        devices,
        RenewableCleanEnergyDispatch(),
    )

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
    energy_contribution_constraint!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.feedforward!(optimization_container, devices, model, PSI.get_feedforward(model))

    # Cost Function
    PSI.cost_function!(optimization_container, devices, model, S)

    return
end

function PSI.construct_device!(
    optimization_container::PSI.OptimizationContainer,
    sys::PSY.System,
    model::PSI.DeviceModel{R, RenewableEmisDispatch},
    ::Type{S},
) where {R <: PSY.RenewableGen, S <: PM.AbstractPowerModel}
    devices = PSI.get_available_components(R, sys)

    if !PSI.validate_available_devices(R, devices)
        return
    end

    # Variables
    PSI.add_variables!(
        optimization_container,
        PSI.ActivePowerVariable,
        devices,
        RenewableEmisDispatch(),
    )
    PSI.add_variables!(
        optimization_container,
        PSI.ReactivePowerVariable,
        devices,
        RenewableEmisDispatch(),
    )

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
    PSI.cost_function!(optimization_container, devices, model, S)

    return
end

function PSI.construct_device!(
    optimization_container::PSI.OptimizationContainer,
    sys::PSY.System,
    model::PSI.DeviceModel{R, RenewableEmisDispatch},
    ::Type{S},
) where {R <: PSY.RenewableGen, S <: PM.AbstractActivePowerModel}
    devices = PSI.get_available_components(R, sys)

    if !PSI.validate_available_devices(R, devices)
        return
    end

    # Variables
    PSI.add_variables!(
        optimization_container,
        PSI.ActivePowerVariable,
        devices,
        RenewableEmisDispatch(),
    )

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
    PSI.cost_function!(optimization_container, devices, model, S)

    return
end
