# For DC Power only. Implements constraints
function PSI.construct_device!(
    optimization_container::PSI.OptimizationContainer,
    sys::PSY.System,
    model::PSI.DeviceModel{B, StaticBranchFlowCost},
    ::Type{S},
) where {B <: PSY.ACBranch, S <: PSI.NFAPowerModel}
    devices = PSI.get_available_components(B, sys)
    if !PSI.validate_available_devices(B, devices)
        return
    end
    PSI.branch_rate_constraints!(
        optimization_container,
        devices,
        model,
        S,
        PSI.get_feedforward(model),
    )
    # Cost Function
    PSI.cost_function!(optimization_container, devices, model, S)

    return
end

PSI.construct_device!(
    ::PSI.OptimizationContainer,
    ::PSY.System,
    ::PSI.DeviceModel{<:PSY.ACBranch, StaticBranchFlowCost},
    ::Union{Type{PSI.CopperPlatePowerModel}, Type{PSI.AreaBalancePowerModel}},
) = nothing
