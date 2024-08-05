function PSI.construct_device!(
    optimization_container::PSI.OptimizationContainer,
    sys::PSY.System,
    model::PSI.DeviceModel{T, ThermalInertiaBasicUnitCommitment},
    ::Type{S},
) where {T <: PSY.ThermalGen, S <: PM.AbstractPowerModel}
    devices = PSI.get_available_components(T, sys)

    if !PSI.validate_available_devices(T, devices)
        return
    end

    # Variables
    PSI.add_variables!(
        optimization_container,
        PSI.ActivePowerVariable,
        devices,
        ThermalInertiaBasicUnitCommitment(),
    )
    PSI.add_variables!(
        optimization_container,
        PSI.ReactivePowerVariable,
        devices,
        ThermalInertiaBasicUnitCommitment(),
    )
    PSI.add_variables!(
        optimization_container,
        PSI.OnVariable,
        devices,
        ThermalInertiaBasicUnitCommitment(),
    )
    PSI.add_variables!(
        optimization_container,
        PSI.StartVariable,
        devices,
        ThermalInertiaBasicUnitCommitment(),
    )
    PSI.add_variables!(
        optimization_container,
        PSI.StopVariable,
        devices,
        ThermalInertiaBasicUnitCommitment(),
    )

    # Initial Conditions
    PSI.initial_conditions!(
        optimization_container,
        devices,
        ThermalInertiaBasicUnitCommitment(),
    )

    # Constraints
    # TODO: active_power_constraints
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
    PSI.commitment_constraints!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.feedforward!(optimization_container, devices, model, PSI.get_feedforward(model))

    # Cost Function
    PSI.cost_function!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )

    return
end

"""
This function creates the model for a full thermal dispatch formulation depending on combination of devices, device_formulation and system_formulation
"""
function PSI.construct_device!(
    optimization_container::PSI.OptimizationContainer,
    sys::PSY.System,
    model::PSI.DeviceModel{T, ThermalInertiaBasicUnitCommitment},
    ::Type{S},
) where {T <: PSY.ThermalGen, S <: PM.AbstractActivePowerModel}
    devices = PSI.get_available_components(T, sys)

    if !PSI.validate_available_devices(T, devices)
        return
    end

    # Variables
    PSI.add_variables!(
        optimization_container,
        PSI.ActivePowerVariable,
        devices,
        ThermalInertiaBasicUnitCommitment(),
    )
    PSI.add_variables!(
        optimization_container,
        PSI.OnVariable,
        devices,
        ThermalInertiaBasicUnitCommitment(),
    )
    PSI.add_variables!(
        optimization_container,
        PSI.StartVariable,
        devices,
        ThermalInertiaBasicUnitCommitment(),
    )
    PSI.add_variables!(
        optimization_container,
        PSI.StopVariable,
        devices,
        ThermalInertiaBasicUnitCommitment(),
    )

    # Initial Conditions
    PSI.initial_conditions!(
        optimization_container,
        devices,
        ThermalInertiaBasicUnitCommitment(),
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
    PSI.commitment_constraints!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.feedforward!(optimization_container, devices, model, PSI.get_feedforward(model))

    # Cost Function
    PSI.cost_function!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )

    return
end

function PSI.construct_device!(
    optimization_container::PSI.OptimizationContainer,
    sys::PSY.System,
    model::PSI.DeviceModel{T, D},
    ::Type{S},
) where {
    T <: PSY.ThermalGen,
    D <: ThermalEmisStandardUnitCommitment,
    S <: PM.AbstractPowerModel,
}
    devices = PSI.get_available_components(T, sys)

    if !PSI.validate_available_devices(T, devices)
        return
    end

    # Variables
    PSI.add_variables!(optimization_container, PSI.ActivePowerVariable, devices, D())
    PSI.add_variables!(optimization_container, PSI.ReactivePowerVariable, devices, D())
    PSI.add_variables!(optimization_container, PSI.OnVariable, devices, D())
    PSI.add_variables!(optimization_container, PSI.StartVariable, devices, D())
    PSI.add_variables!(optimization_container, PSI.StopVariable, devices, D())

    # Aux Variables
    PSI.add_variables!(optimization_container, PSI.TimeDurationOn, devices, D())
    PSI.add_variables!(optimization_container, PSI.TimeDurationOff, devices, D())

    # Initial Conditions
    PSI.initial_conditions!(optimization_container, devices, D())

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
    PSI.commitment_constraints!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.ramp_constraints!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.time_constraints!(
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
    PSI.cost_function!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )

    return
end

"""
This function creates the model for a full thermal dispatch formulation depending on combination of devices, device_formulation and system_formulation
"""
function PSI.construct_device!(
    optimization_container::PSI.OptimizationContainer,
    sys::PSY.System,
    model::PSI.DeviceModel{T, D},
    ::Type{S},
) where {
    T <: PSY.ThermalGen,
    D <: ThermalEmisStandardUnitCommitment,
    S <: PM.AbstractActivePowerModel,
}
    devices = PSI.get_available_components(T, sys)

    if !PSI.validate_available_devices(T, devices)
        return
    end

    # Variables
    PSI.add_variables!(optimization_container, PSI.ActivePowerVariable, devices, D())
    PSI.add_variables!(optimization_container, PSI.OnVariable, devices, D())
    PSI.add_variables!(optimization_container, PSI.StartVariable, devices, D())
    PSI.add_variables!(optimization_container, PSI.StopVariable, devices, D())

    # Aux Variables
    PSI.add_variables!(optimization_container, PSI.TimeDurationOn, devices, D())
    PSI.add_variables!(optimization_container, PSI.TimeDurationOff, devices, D())

    # Initial Conditions
    PSI.initial_conditions!(optimization_container, devices, D())

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
    PSI.commitment_constraints!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.ramp_constraints!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.time_constraints!(
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
    PSI.cost_function!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )

    return
end

function PSI.construct_device!(
    optimization_container::PSI.OptimizationContainer,
    sys::PSY.System,
    model::PSI.DeviceModel{T, D},
    ::Type{S},
) where {
    T <: PSY.ThermalGen,
    D <: ThermalCleanStandardUnitCommitment,
    S <: PM.AbstractPowerModel,
}
    devices = PSI.get_available_components(T, sys)

    if !PSI.validate_available_devices(T, devices)
        return
    end

    # Variables
    PSI.add_variables!(optimization_container, PSI.ActivePowerVariable, devices, D())
    PSI.add_variables!(optimization_container, PSI.ReactivePowerVariable, devices, D())
    PSI.add_variables!(optimization_container, PSI.OnVariable, devices, D())
    PSI.add_variables!(optimization_container, PSI.StartVariable, devices, D())
    PSI.add_variables!(optimization_container, PSI.StopVariable, devices, D())

    # Aux Variables
    PSI.add_variables!(optimization_container, PSI.TimeDurationOn, devices, D())
    PSI.add_variables!(optimization_container, PSI.TimeDurationOff, devices, D())

    # Initial Conditions
    PSI.initial_conditions!(optimization_container, devices, D())

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
    PSI.commitment_constraints!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.ramp_constraints!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.time_constraints!(
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
    PSI.cost_function!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )

    return
end

"""
This function creates the model for a full thermal dispatch formulation depending on combination of devices, device_formulation and system_formulation
"""
function PSI.construct_device!(
    optimization_container::PSI.OptimizationContainer,
    sys::PSY.System,
    model::PSI.DeviceModel{T, D},
    ::Type{S},
) where {
    T <: PSY.ThermalGen,
    D <: ThermalCleanStandardUnitCommitment,
    S <: PM.AbstractActivePowerModel,
}
    devices = PSI.get_available_components(T, sys)

    if !PSI.validate_available_devices(T, devices)
        return
    end

    # Variables
    PSI.add_variables!(optimization_container, PSI.ActivePowerVariable, devices, D())
    PSI.add_variables!(optimization_container, PSI.OnVariable, devices, D())
    PSI.add_variables!(optimization_container, PSI.StartVariable, devices, D())
    PSI.add_variables!(optimization_container, PSI.StopVariable, devices, D())

    # Aux Variables
    PSI.add_variables!(optimization_container, PSI.TimeDurationOn, devices, D())
    PSI.add_variables!(optimization_container, PSI.TimeDurationOff, devices, D())

    # Initial Conditions
    PSI.initial_conditions!(optimization_container, devices, D())

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
    PSI.commitment_constraints!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.ramp_constraints!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    PSI.time_constraints!(
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
    PSI.cost_function!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )

    return
end
