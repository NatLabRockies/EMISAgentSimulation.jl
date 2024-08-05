# function update_quadratic_cost!(
#     optimization_container::PSI.OptimizationContainer,
#     problem::PSI.OperationsProblem,
#     sim::PSI.Simulation,
#     spec::PSI.AddCostSpec,
#     service::SR,
#     component_name::String,
# ) where {SR <: PSY.Reserve}
#     time_steps = PSI.model_time_steps(optimization_container)
#     horizon = length(time_steps)
#     use_forecast_data = PSI.model_uses_forecasts(optimization_container)
#     if !use_forecast_data
#         error("QuadraticCostRampReserve is only supported with forecast")
#     end

#     initial_forecast_time = PSI.get_simulation_time(sim, PSI.get_simulation_number(problem))
#     # use forecast to update parameters
#     variable_cost_forecast =
#         PSI.get_time_series_values!(
#             PSY.Deterministic, 
#             problem, 
#             service, 
#             "variable_cost", 
#             initial_forecast_time,
#             horizon,
#             ignore_scaling_factors = true,
#         )
#     variable_cost_forecast = map(PSY.VariableCost, variable_cost_forecast)
#     # remove existing variables and cost in objective function?

#     for t in time_steps
#         _quadratic_cost!(
#             optimization_container,
#             spec,
#             component_name,
#             variable_cost_forecast[t],
#             t,
#         )
#     end
#     return
# end


# function _quadratic_cost!(
#     optimization_container::PSI.OptimizationContainer,
#     spec::PSI.AddCostSpec,
#     component_name::String,
#     cost_component::PSY.VariableCost{Vector{NTuple{2, Float64}}},
#     time_period::Int,
# )
#     @info("add quadratic cost")
#     resolution = PSI.model_resolution(optimization_container)
#     dt = Dates.value(Dates.Second(resolution)) / PSI.SECONDS_IN_HOUR
#     # If array is full of tuples with zeros return 0.0
#     cost_data = PSY.get_cost(cost_component)
#     if all(iszero.(last.(cost_data)))
#         @debug "All cost terms for component $(component_name) are 0.0"
#         return JuMP.AffExpr(0.0)
#     end

#     var_name = PSI.make_variable_name(spec.variable_type, spec.component_type)
#     base_power = PSI.get_base_power(optimization_container)
#     variable =
#         PSI.get_variable(optimization_container, var_name)[component_name, time_period]
#     settings_ext = PSI.get_ext(PSI.get_settings(optimization_container))
#     export_pwl_vars = PSI.get_export_pwl_vars(optimization_container.settings)
#     @debug export_pwl_vars
#     gen_cost = JuMP.AffExpr(0.0)
#     segvars = Array{JuMP.VariableRef}(undef, length(cost_data))
#     for i in 1:length(cost_data)
#         segvars[i] = JuMP.@variable(
#             optimization_container.JuMPmodel,
#             base_name = "$(variable)_{seg_$i}",
#             start = 0.0,
#             lower_bound = 0.0,
#             upper_bound = PSY.get_breakpoint_upperbounds(cost_data)[i] / base_power
#         )
#         if export_pwl_vars
#             container = PSI._get_pwl_vars_container(optimization_container)
#             container[(component_name, time_period, i)] = segvars[i]
#         end
#         JuMP.add_to_expression!(gen_cost, cost_data[i][1] * base_power * segvars[i])
#         slope =
#             abs(PSY.get_slopes(cost_data)[i]) != Inf ? PSY.get_slopes(cost_data)[i] : 0.0
#         gen_cost += ((1 / 2) * slope) * (base_power * segvars[i]) .^ 2
#     end
#     # JuMP.@constraint(
#     #     optimization_container.JuMPmodel,
#     #     variable == sum([var for var in segvars])
#     # )
#     PSI.add_to_cost_expression!(optimization_container, spec.multiplier * gen_cost * dt)
#     return
# end


# function update_parameter_ordc!(
#     optimization_container::PSI.OptimizationContainer,
#     problem::PSI.OperationsProblem,
#     sim::PSI.Simulation,
#     service::SR,
#     model::PSI.ServiceModel{SR, QuadraticCostRampReserve},
# ) where {SR <: PSY.ReserveDemandCurve}
#     @info("update parameter for ReserveDemandCurve")

#     spec = PSI.AddCostSpec(SR, model.formulation, optimization_container)
#     @debug SR, spec
#     update_quadratic_cost!(optimization_container, problem, sim, spec, service, PSY.get_name(service))

#     return
# end


# function PSI._update_parameters(problem::PSI.OperationsProblem, sim::PSI.Simulation)
#     optimization_container = PSI.get_optimization_container(problem)
#     for container in PSI.iterate_parameter_containers(optimization_container)
#         PSI.update_parameter!(container.update_ref, container, problem, sim)
#     end

#     model = PSI.ServiceModel(PSY.ReserveDemandCurve{PSY.ReserveUp}, QuadraticCostRampReserve)
#     sys = PSI.get_system(problem)
#     for service in PSY.get_components(PSY.ReserveDemandCurve{PSY.ReserveUp}, sys)
#         update_parameter_ordc!(optimization_container, problem, sim, service, model)
#     end

#     return
# end