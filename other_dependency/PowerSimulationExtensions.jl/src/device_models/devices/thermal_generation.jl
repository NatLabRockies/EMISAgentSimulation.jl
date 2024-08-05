
abstract type AbstractInertiaStandardUnitCommitment <: PSI.AbstractStandardUnitCommitment end
struct ThermalInertiaBasicUnitCommitment <: AbstractInertiaStandardUnitCommitment end
struct ThermalInertiaStandardUnitCommitment <: AbstractInertiaStandardUnitCommitment end
struct ThermalEmisStandardUnitCommitment <: AbstractInertiaStandardUnitCommitment end
struct ThermalCleanStandardUnitCommitment <: PSI.AbstractStandardUnitCommitment end

PSI.get_variable_binary(::ActivePowerShortageVariable, ::Type{<:PSY.ThermalGen}, _) = false
PSI.get_variable_lower_bound(::ActivePowerShortageVariable, d::PSY.ThermalGen, _) = 0.0
PSI.get_variable_upper_bound(::ActivePowerShortageVariable, d::PSY.ThermalGen, _) =
    PSY.get_active_power_limits(d).max

function PSI.DeviceRangeConstraintSpec(
    ::Type{<:PSI.RangeConstraint},
    ::Type{<:PSI.VariableType},
    ::Type{T},
    ::Type{<:PSI.AbstractThermalFormulation},
    ::Type{<:PM.AbstractPowerModel},
    feedforward::Union{InertiaFF, EmisFF},
    use_parameters::Bool,
    use_forecasts::Bool,
) where {T <: PSY.ThermalGen}
    return PSI.DeviceRangeConstraintSpec()
end

function PSI.DeviceRangeConstraintSpec(
    ::Type{<:PSI.RangeConstraint},
    ::Type{PSI.ReactivePowerVariable},
    ::Type{T},
    ::Type{<:PSI.AbstractThermalDispatchFormulation},
    ::Type{<:PM.AbstractPowerModel},
    feedforward::Union{InertiaFF, EmisFF},
    use_parameters::Bool,
    use_forecasts::Bool,
) where {T <: PSY.ThermalGen}
    return PSI.DeviceRangeConstraintSpec()
end

function PSI.DeviceRangeConstraintSpec(
    ::Type{<:PSI.RangeConstraint},
    ::Type{PSI.ReactivePowerVariable},
    ::Type{T},
    ::Type{<:PSI.AbstractThermalUnitCommitment},
    ::Type{<:PM.AbstractPowerModel},
    feedforward::Union{InertiaFF, EmisFF},
    use_parameters::Bool,
    use_forecasts::Bool,
) where {T <: PSY.ThermalGen}
    return PSI.DeviceRangeConstraintSpec()
end

function PSI.commitment_constraints!(
    optimization_container::PSI.OptimizationContainer,
    devices::IS.FlattenIteratorWrapper{T},
    model::PSI.DeviceModel{T, D},
    ::Type{S},
    feedforward::Union{Nothing, PSI.AbstractAffectFeedForward},
) where {
    T <: PSY.ThermalGen,
    D <: AbstractInertiaStandardUnitCommitment,
    S <: PM.AbstractPowerModel,
}
    if _has_inertia_service(model)
        service_model = _get_inertia_service_model(model)
        service = _get_services(first(devices), service_model)[1]
        constraint_info = Vector{InertiaCommitmentConstraintInfo}(undef, length(devices))
        initial_conditions = PSI.get_initial_conditions(
            optimization_container,
            PSI.ICKey(PSI.DeviceStatus, T),
        )
        for (idx, ic) in enumerate(initial_conditions)
            constraint_info[idx] = InertiaCommitmentConstraintInfo(
                PSY.get_name(ic.device),
                ic,
                _get_inertia(ic.device),
                PSY.get_base_power(ic.device),
            )
        end
        device_commitment_inertia!(
            optimization_container,
            constraint_info,
            PSI.make_constraint_name(PSI.COMMITMENT, T),
            (
                PSI.make_variable_name(PSI.StartVariable, T),
                PSI.make_variable_name(PSI.StopVariable, T),
                PSI.make_variable_name(PSI.OnVariable, T),
            ),
            PSI.make_variable_name(PSY.get_name(service), typeof(service)),
        )

    else
        PSI.device_commitment!(
            optimization_container,
            PSI.get_initial_conditions(
                optimization_container,
                PSI.ICKey(PSI.DeviceStatus, T),
            ),
            PSI.make_constraint_name(PSI.COMMITMENT, T),
            (
                PSI.make_variable_name(PSI.StartVariable, T),
                PSI.make_variable_name(PSI.StopVariable, T),
                PSI.make_variable_name(PSI.OnVariable, T),
            ),
        )
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
    T <: PSY.ThermalGen,
    D <: Union{ThermalEmisStandardUnitCommitment, ThermalCleanStandardUnitCommitment},
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
