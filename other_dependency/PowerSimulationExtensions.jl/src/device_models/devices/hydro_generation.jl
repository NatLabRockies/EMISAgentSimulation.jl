abstract type AbstractInertiaUnitCommitment <: PSI.AbstractHydroUnitCommitment end
struct HydroInertiaCommitmentRunOfRiver <: AbstractInertiaUnitCommitment end
struct HydroCleanEnergyRunOfRiver <: PSI.AbstractHydroUnitCommitment end
struct HydroEmisCommitmentRunOfRiver <: AbstractInertiaUnitCommitment end

PSI.get_variable_binary(::ActivePowerShortageVariable, ::Type{<:PSY.HydroGen}, _) = false
PSI.get_variable_lower_bound(::ActivePowerShortageVariable, d::PSY.HydroGen, _) = 0.0
PSI.get_variable_upper_bound(::ActivePowerShortageVariable, d::PSY.HydroGen, _) =
    PSY.get_active_power_limits(d).max

function PSI.DeviceRangeConstraintSpec(
    ::Type{<:PSI.RangeConstraint},
    ::Type{PSI.ActivePowerVariable},
    ::Type{T},
    ::Type{<:AbstractInertiaUnitCommitment},
    ::Type{<:PM.AbstractPowerModel},
    feedforward::Union{InertiaFF, EmisFF},
    use_parameters::Bool,
    use_forecasts::Bool,
) where {T <: PSY.HydroGen}
    return PSI.DeviceRangeConstraintSpec(;
        range_constraint_spec = PSI.RangeConstraintSpec(;
            constraint_name = PSI.make_constraint_name(
                PSI.RangeConstraint,
                PSI.ActivePowerVariable,
                T,
            ),
            variable_name = PSI.make_variable_name(PSI.ActivePowerVariable, T),
            bin_variable_names = [PSI.make_variable_name(PSI.OnVariable, T)],
            limits_func = x -> PSY.get_active_power_limits(x),
            constraint_func = PSI.device_semicontinuousrange!,
            constraint_struct = PSI.DeviceRangeConstraintInfo,
        ),
    )
end

function inertia_constraints!(
    optimization_container::PSI.OptimizationContainer,
    devices::IS.FlattenIteratorWrapper{T},
    model::PSI.DeviceModel{T, D},
    ::Type{S},
    feedforward::Union{Nothing, PSI.AbstractAffectFeedForward},
) where {T <: PSY.HydroGen, D <: AbstractInertiaUnitCommitment, S <: PM.AbstractPowerModel}
    if _has_inertia_service(model)
        service_model = _get_inertia_service_model(model)
        service = _get_services(first(devices), service_model)[1]

        time_steps = PSI.model_time_steps(optimization_container)
        use_parameters = PSI.model_has_parameters(optimization_container)
        constraint_info = Vector{InertiaCommitmentConstraintInfo}(undef, length(devices))
        for (idx, d) in enumerate(devices)
            constraint_info[idx] = InertiaCommitmentConstraintInfo(
                PSY.get_name(d),
                nothing,
                _get_inertia(d),
                PSY.get_base_power(d),
            )
        end

        hydro_device_inertia!(
            optimization_container,
            constraint_info,
            PSI.make_constraint_name(INERTIA_LIMIT, T),
            PSI.make_variable_name(PSI.ACTIVE_POWER, T),
            PSI.make_variable_name(PSY.get_name(service), typeof(service)),
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
    T <: PSY.HydroGen,
    D <: Union{HydroCleanEnergyRunOfRiver, HydroEmisCommitmentRunOfRiver},
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
