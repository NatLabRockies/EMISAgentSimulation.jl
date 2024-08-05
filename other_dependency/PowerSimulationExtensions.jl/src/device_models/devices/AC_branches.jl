struct StaticBranchFlowCost <: PSI.AbstractBranchFormulation end

function PSI.cost_function!(
    optimization_container::PSI.OptimizationContainer,
    devices::PSI.IS.FlattenIteratorWrapper{T},
    ::PSI.DeviceModel{T, U},
    ::Type{<:PSI.PM.AbstractPowerModel},
    feedforward::Union{Nothing, PSI.AbstractAffectFeedForward} = nothing,
) where {T <: PSY.ACBranch, U <: StaticBranchFlowCost}
    for d in devices
        spec = PSI.AddCostSpec(T, U, optimization_container)
        @debug T, spec
        PSI.add_to_cost!(optimization_container, spec, PSY.get_ext(d)["flow_cost"], d)
    end
    return
end

function PSI.AddCostSpec(
    ::Type{T},
    ::Type{StaticBranchFlowCost},
    ::PSI.OptimizationContainer,
) where {T <: PSY.ACBranch}
    cost_function = x -> (x === nothing ? 1.0 : PSY.get_variable(x))
    return PSI.AddCostSpec(;
        variable_type = PSI.FlowActivePowerVariable,
        component_type = T,
        variable_cost = cost_function,
    )
end
