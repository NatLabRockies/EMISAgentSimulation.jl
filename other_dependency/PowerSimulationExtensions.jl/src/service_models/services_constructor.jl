function PSI.construct_service!(
    optimization_container::PSI.OptimizationContainer,
    services::Vector{SR},
    sys::PSY.System,
    model::PSI.ServiceModel{SR, QuadraticCostRampReserve},
    devices_template::Dict{Symbol, PSI.DeviceModel},
    incompatible_device_types::Vector{<:DataType},
) where {SR <: PSY.Reserve}
    services_mapping = PSY.get_contributing_device_mapping(sys)
    time_steps = PSI.model_time_steps(optimization_container)
    names = [PSY.get_name(s) for s in services]
    # Does not use the standard implementation of add_variable!()
    PSI.add_variable!(
        optimization_container,
        PSI.ServiceRequirementVariable(),
        services,
        QuadraticCostRampReserve(),
    )
    PSI.add_cons_container!(
        optimization_container,
        PSI.make_constraint_name(PSI.REQUIREMENT, SR),
        names,
        time_steps,
    )

    for service in services
        contributing_devices =
            services_mapping[(
                type = typeof(service),
                name = PSY.get_name(service),
            )].contributing_devices
        if !isempty(incompatible_device_types)
            contributing_devices =
                [d for d in contributing_devices if typeof(d) ∉ incompatible_device_types]
        end
        # Variables
        PSI.add_variables!(
            optimization_container,
            PSI.ActiveServiceVariable,
            service,
            contributing_devices,
            QuadraticCostRampReserve(),
        )
        # Constraints
        PSI.service_requirement_constraint!(optimization_container, service, model)
        PSI.ramp_constraints!(optimization_container, service, contributing_devices, model)
        PSI.modify_device_model!(devices_template, model, contributing_devices)

        # Cost Function
        PSI.cost_function!(optimization_container, service, model)
    end
    return
end

function PSI.construct_service!(
    optimization_container::PSI.OptimizationContainer,
    services::Vector{SR},
    sys::PSY.System,
    model::PSI.ServiceModel{SR, VariableInertiaReserve},
    devices_template::Dict{Symbol, PSI.DeviceModel},
    incompatible_device_types::Vector{<:DataType},
) where {SR <: PSY.Reserve}
    services_mapping = PSY.get_contributing_device_mapping(sys)
    time_steps = PSI.model_time_steps(optimization_container)
    names = [PSY.get_name(s) for s in services]

    if PSI.model_has_parameters(optimization_container)
        container = PSI.add_param_container!(
            optimization_container,
            PSI.UpdateRef{SR}("service_requirement", "requirement"),
            names,
            time_steps,
        )
    end

    PSI.add_cons_container!(
        optimization_container,
        PSI.make_constraint_name(PSI.REQUIREMENT, SR),
        names,
        time_steps,
    )

    for service in services
        contributing_devices =
            services_mapping[(
                type = typeof(service),
                name = PSY.get_name(service),
            )].contributing_devices
        if !isempty(incompatible_device_types)
            contributing_devices =
                [d for d in contributing_devices if typeof(d) ∉ incompatible_device_types]
        end
        # Variables
        PSI.add_variables!(
            optimization_container,
            InertiaServiceVariable,
            service,
            contributing_devices,
            VariableInertiaReserve(),
        )
        # Constraints
        PSI.service_requirement_constraint!(optimization_container, service, model)
        PSI.modify_device_model!(devices_template, model, contributing_devices)
        # Cost Function
        PSI.cost_function!(optimization_container, service, model)
    end
    return
end

function PSI.construct_service!(
    optimization_container::PSI.OptimizationContainer,
    services::Vector{SR},
    sys::PSY.System,
    model::PSI.ServiceModel{SR, EnergyRequirementReserve},
    devices_template::Dict{Symbol, PSI.DeviceModel},
    incompatible_device_types::Vector{<:DataType},
) where {SR <: PSY.Reserve}
    services_mapping = PSY.get_contributing_device_mapping(sys)
    time_steps = PSI.model_time_steps(optimization_container)
    names = [PSY.get_name(s) for s in services]

    if PSI.model_has_parameters(optimization_container)
        container = PSI.add_param_container!(
            optimization_container,
            PSI.UpdateRef{SR}("service_requirement", "requirement"),
            names,
            time_steps,
        )
    end

    PSI.add_cons_container!(
        optimization_container,
        PSI.make_constraint_name(PSI.REQUIREMENT, SR),
        names,
    )

    for service in services
        contributing_devices =
            services_mapping[(
                type = typeof(service),
                name = PSY.get_name(service),
            )].contributing_devices
        if !isempty(incompatible_device_types)
            contributing_devices =
                [d for d in contributing_devices if typeof(d) ∉ incompatible_device_types]
        end
        # Variables
        PSI.add_variables!(
            optimization_container,
            PSI.ActiveServiceVariable,
            service,
            contributing_devices,
            EnergyRequirementReserve(),
        )
        # Constraints
        PSI.service_requirement_constraint!(optimization_container, service, model)
        PSI.modify_device_model!(devices_template, model, contributing_devices)
        # Cost Function
        PSI.cost_function!(optimization_container, service, model)
    end
    return
end
