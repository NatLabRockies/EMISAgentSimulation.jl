# ORDC in a multi-step PSI Simulation, with the HDF5 simulation store.
#
# test_PSI_build_solve.jl solves standalone DecisionModels, which never write the simulation
# store or update time-varying parameters between steps. This file runs a short Simulation
# (2 steps) per stage template with ORDC enabled, which exercises both:
#   - the ORDC curve parameters (DecrementalPiecewiseLinearSlope/Breakpoint) being updated for
#     each step from the "variable_cost" forecast, and
#   - the store writing those parameters. siip_simulation_definition.jl documents a PSI HDF5
#     dimension error ("Number of indices does not match dimension of Dataspace") when ORDC is in
#     ED; no test has checked whether that still happens, or whether it also affects MD/UC.
# It then reads the realized ORDC duals and variables the way EMIS does (read_realized_duals /
# read_realized_variables with the keys from create_simulation) and checks them.
#
# Heavy: full-scale fixtures (test/test_systems) and Xpress MIP solves. Disabled in runtests.jl
# (DISABLED_TEST_FILES); run on a compute node with an Xpress license:
#   julia --project=test test/test_ordc_simulation.jl
# or from the REPL:
#   include("test/test_ordc_simulation.jl")

include(joinpath(@__DIR__, "includes.jl"))
include(joinpath(@__DIR__, "psi_test_utils.jl"))

const ORDC_SIM_STEPS = 2

function _run_ordc_simulation(template::PSI.ProblemTemplate, sys::PSY.System, name::String)
    model = _test_decision_model(template, sys, name)
    models = PSI.SimulationModels(; decision_models = [model])
    sequence = PSI.SimulationSequence(;
        models = models,
        ini_cond_chronology = PSI.InterProblemChronology(),
    )
    sim = PSI.Simulation(;
        name = "ordc_$(name)",
        steps = ORDC_SIM_STEPS,
        models = models,
        sequence = sequence,
        simulation_folder = mktempdir(),
    )
    @test PSI.build!(sim) == PSI.SimulationBuildStatus.BUILT
    status = PSI.execute!(sim)
    @test status == PSI.RunStatus.SUCCESSFULLY_FINALIZED
    return sim, status
end

@testset "ORDC in a multi-step PSI Simulation (HDF5 store)" begin
    cases = [
        ("UC", "sys_UC_year1.json", () -> EMISAgentSimulation.create_uc_template()),
        ("MD", "sys_MD_year1.json", () -> EMISAgentSimulation.create_md_template()),
        # The template a case with an Inertia service gets (create_ed_template only checks
        # that inertia_product is non-empty).
        ("ED", "sys_ED_year1.json", () -> EMISAgentSimulation.create_ed_template([:inertia])),
    ]
    for (name, system_file, make_template) in cases
        @testset "$(name) with ORDC, $(ORDC_SIM_STEPS) steps" begin
            sys = _load_test_system(system_file)
            sim, status = _run_ordc_simulation(make_template(), sys, name)
            if status == PSI.RunStatus.SUCCESSFULLY_FINALIZED
            results = PSI.get_decision_problem_results(PSI.SimulationResults(sim), name)
            dual_names = PSI.list_dual_names(results)
            variable_names = PSI.list_variable_names(results)
            duals = PSI.read_realized_duals(results, dual_names)
            variables = PSI.read_realized_variables(results, variable_names)

            interval = PSY.get_forecast_interval(sys)
            first_start = first(PSY.get_forecast_initial_times(sys))
            starts = [first_start + (k - 1) * interval for k in 1:ORDC_SIM_STEPS]
            check_ordc_results(sys, duals, variables, starts)

            @testset "realized ORDC prices cover every step" begin
                steps_per_window = Dates.Millisecond(interval) ÷ Dates.Millisecond(PSY.get_time_series_resolutions(sys)[1])
                for service in PSY.get_components(PSY.ReserveDemandCurve{PSY.ReserveUp}, sys)
                    key = ordc_dual_key(PSY.get_name(service))
                    haskey(duals, key) || continue
                    @test DataFrames.nrow(duals[key]) >= ORDC_SIM_STEPS * steps_per_window
                end
            end
            end
        end
    end
end
