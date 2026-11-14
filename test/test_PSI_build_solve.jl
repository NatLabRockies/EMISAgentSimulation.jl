# Build + solve tests for the UC/MD/ED PSI templates (src/PSI_definitions.jl,
# markets_simulation/siip_simulation_definition.jl) against the bundled
# test/test_systems fixtures.
#
# Goal: catch template/formulation regressions (wrong component type, missing
# device model, broken service model config) by actually constructing and
# solving a PSI DecisionModel, without running a full multi-year EMIS case.
# For the ORDC products (ReserveDemandCurve "Synchronous"/"Primary", carried by the
# fixtures), also check the result keys and prices EMIS reads after a solve.
#
# These are heavier than test_PSI.jl: each @testset loads a full-scale
# serialized PSY.System (thousands of components) and solves a UC/MD/ED
# DecisionModel with Xpress. Expect real wall-clock time (minutes, not
# seconds) — this is an integration-style check, not a fast unit test.
# Disabled in runtests.jl (DISABLED_TEST_FILES); needs an Xpress license.
#
# Run from REPL:
#   include("test/test_PSI_build_solve.jl")

include(joinpath(@__DIR__, "includes.jl"))
include(joinpath(@__DIR__, "psi_test_utils.jl"))

"""ORDC result checks on a solved standalone DecisionModel (first forecast window)."""
function _check_model_ordc_results(model::PSI.DecisionModel, sys::PSY.System)
    res = PSI.OptimizationProblemResults(model)
    duals = Dict(k => PSI.read_dual(res, k) for k in PSI.list_dual_names(res))
    variables = Dict(k => PSI.read_variable(res, k) for k in PSI.list_variable_names(res))
    start = first(PSY.get_forecast_initial_times(sys))
    check_ordc_results(sys, duals, variables, [start])
end

@testset "PSI template build+solve against bundled test systems" begin

    @testset "UC template + DA test system" begin
        sys = _load_test_system("sys_UC_year1.json")
        template = EMISAgentSimulation.create_uc_template()

        @testset "no unmodeled component types" begin
            @test isempty(_unmodeled_component_types(template, sys))
        end

        model = _test_decision_model(template, sys, "UC_test")
        _build_and_solve!(model)
        PSI.get_run_status(model) == PSI.RunStatus.SUCCESSFULLY_FINALIZED &&
            _check_model_ordc_results(model, sys)
    end

    @testset "MD template + MD test system" begin
        sys = _load_test_system("sys_MD_year1.json")
        template = EMISAgentSimulation.create_md_template()

        @testset "no unmodeled component types" begin
            @test isempty(_unmodeled_component_types(template, sys))
        end

        model = _test_decision_model(template, sys, "MD_test")
        _build_and_solve!(model)
        PSI.get_run_status(model) == PSI.RunStatus.SUCCESSFULLY_FINALIZED &&
            _check_model_ordc_results(model, sys)
    end

    @testset "ED template (no inertia) + RT test system" begin
        sys = _load_test_system("sys_ED_year1.json")
        inertia_product = collect(PSY.get_components_by_name(PSY.Service, sys, "Inertia"))
        template = EMISAgentSimulation.create_ed_template(inertia_product)

        @testset "system has no Inertia service (base ED path)" begin
            # If this ever fails, the test system gained an Inertia service and
            # the ED template would take the ORDC branch — see the next testset.
            @test isempty(inertia_product)
        end

        @testset "no unmodeled component types" begin
            # The fixture carries ReserveDemandCurve services, which the base ED
            # template does not model (ORDC is added to ED only with Inertia).
            unmodeled = _unmodeled_component_types(template, sys)
            @test isempty(filter(t -> t != PSY.ReserveDemandCurve{PSY.ReserveUp}, unmodeled))
        end

        model = _test_decision_model(template, sys, "ED_test")
        _build_and_solve!(model)
    end

    @testset "ED template (ORDC branch, as with an Inertia service) + RT test system" begin
        # create_ed_template only checks that inertia_product is non-empty; pass a stand-in
        # to build the template a case with an Inertia service would get.
        sys = _load_test_system("sys_ED_year1.json")
        template = EMISAgentSimulation.create_ed_template([:inertia])

        @testset "no unmodeled component types" begin
            @test isempty(_unmodeled_component_types(template, sys))
        end

        model = _test_decision_model(template, sys, "ED_ordc_test")
        _build_and_solve!(model)
        PSI.get_run_status(model) == PSI.RunStatus.SUCCESSFULLY_FINALIZED &&
            _check_model_ordc_results(model, sys)
    end

end
