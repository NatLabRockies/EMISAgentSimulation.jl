function quadratic_cost!(
    optimization_container::PSI.OptimizationContainer,
    spec::PSI.AddCostSpec,
    component_name::String,
    cost_component::PSY.VariableCost{Vector{NTuple{2, Float64}}},
    time_period::Int,
)
    @info("add quadratic cost")
    resolution = PSI.model_resolution(optimization_container)
    dt = Dates.value(Dates.Second(resolution)) / PSI.SECONDS_IN_HOUR
    # If array is full of tuples with zeros return 0.0
    cost_data = PSY.get_cost(cost_component)
    if all(iszero.(last.(cost_data)))
        @debug "All cost terms for component $(component_name) are 0.0"
        return JuMP.AffExpr(0.0)
    end

    var_name = PSI.make_variable_name(spec.variable_type, spec.component_type)
    base_power = PSI.get_base_power(optimization_container)
    variable =
        PSI.get_variable(optimization_container, var_name)[component_name, time_period]
    settings_ext = PSI.get_ext(PSI.get_settings(optimization_container))
    export_pwl_vars = PSI.get_export_pwl_vars(optimization_container.settings)
    @debug export_pwl_vars
    gen_cost = JuMP.AffExpr(0.0)
    segvars = Array{JuMP.VariableRef}(undef, length(cost_data))
    for i in 1:length(cost_data)
        segvars[i] = JuMP.@variable(
            optimization_container.JuMPmodel,
            base_name = "$(variable)_{seg_$i}",
            start = 0.0,
            lower_bound = 0.0,
            upper_bound = PSY.get_breakpoint_upperbounds(cost_data)[i] / base_power
        )
        if export_pwl_vars
            container = PSI._get_pwl_vars_container(optimization_container)
            container[(component_name, time_period, i)] = segvars[i]
        end
        JuMP.add_to_expression!(gen_cost, cost_data[i][1] * base_power * segvars[i])
        slope =
            abs(PSY.get_slopes(cost_data)[i]) != Inf ? PSY.get_slopes(cost_data)[i] : 0.0
        gen_cost += ((1 / 2) * slope) * (base_power * segvars[i]) .^ 2
    end
    JuMP.@constraint(
        optimization_container.JuMPmodel,
        variable == sum([var for var in segvars])
    )
    PSI.add_to_cost_expression!(optimization_container, spec.multiplier * gen_cost * dt)
    return
end

function PSI.add_to_cost!(
    optimization_container::PSI.OptimizationContainer,
    spec::PSI.AddCostSpec,
    cost_data::Float64,
    component::PSY.Component,
)
    component_name = PSY.get_name(component)
    time_steps = PSI.model_time_steps(optimization_container)
    for t in time_steps
        PSI.variable_cost!(
            optimization_container,
            spec,
            component_name,
            PSY.VariableCost(cost_data),
            t,
        )
    end
    return
end
