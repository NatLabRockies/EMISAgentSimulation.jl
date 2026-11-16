# Shared helpers for the tests that build and solve PSI models against the bundled
# test/test_systems fixtures (test_PSI_build_solve.jl, test_ordc_simulation.jl).
# Safe to include more than once.

const TEST_SYSTEMS_DIR = joinpath(@__DIR__, "test_systems")

# Same Xpress settings as production (run_gs_15_years.jl). NOTE: MAXTIME is
# 7200s (2hr) there — kept identical for parity, but that means a stalled
# solve here can run that long before Xpress itself gives up.
function _test_solver()
    return JuMP.optimizer_with_attributes(
        Xpress.Optimizer,
        "MIPRELSTOP" => 1e-2,
        "BARGAPSTOP" => 1e-4,
        "BARDUALSTOP" => 1e-4,
        "BARPRIMALSTOP" => 1e-4,
        "MATRIXTOL" => 1e-8,
        "BARORDER" => 1,
        "CHOLESKYTOL" => 1e-20,
        "CHOLESKYALG" => 1,
        "BARTHREADS" => 8,
        "SCALING" => 2,
        "BARSTEPSTOP" => 1e-10,
        "OUTPUTLOG" => 0,
        "PRESOLVE" => 1,
        "MAXTIME" => 7200,
        "NUMERICALEMPHASIS" => 1,
    )
end

function _load_test_system(json_name::String)
    return PSY.System(joinpath(TEST_SYSTEMS_DIR, json_name); runchecks = false)
end

# Matches the kwargs create_problem() uses in production (siip_simulation_definition.jl)
# for every problem type. `initialize_model = false` in particular is required: without
# it, PSI.DecisionModel defaults to running an internal warm-start sub-solve at build
# time (build_initial_conditions! + initialize!) that these standalone (non-Simulation)
# models aren't set up for and that fails with "Optimizer returned NO_SOLUTION".
function _test_decision_model(template::PSI.ProblemTemplate, sys::PSY.System, name::String)
    return PSI.DecisionModel(
        template,
        sys;
        optimizer = _test_solver(),
        name = name,
        optimizer_solve_log_print = false,
        warm_start = true,
        calculate_conflict = true,
        store_variable_names = true,
        export_pwl_vars = true,
        initialize_model = false,
    )
end

# PSI only @warns (doesn't error) when a system component type has no
# DeviceModel/ServiceModel in the template — those components are silently
# dropped from the optimization instead of failing the build. This makes that
# failure mode a hard test failure.
function _unmodeled_component_types(template::PSI.ProblemTemplate, sys::PSY.System)
    modeled_types = Set(PSI.get_component_types(template))
    return [t for t in PSY.get_existing_component_types(sys) if !(t in modeled_types)]
end

function _build_and_solve!(model::PSI.DecisionModel)
    @testset "model builds" begin
        build_status = PSI.build!(model; output_dir = mktempdir())
        @test build_status == PSI.ModelBuildStatus.BUILT
    end

    @testset "model solves" begin
        PSI.solve!(model; output_dir = mktempdir())
        @test PSI.get_run_status(model) == PSI.RunStatus.SUCCESSFULLY_FINALIZED
    end
end

# ── ORDC result keys ────────────────────────────────────────────────────────────────────────
# The exact strings siip_simulation_definition.jl reads to get realized ORDC prices
# (create_simulation) and reserve shares (update_realized_reserve_perc!). If PSI names a result
# differently, EMIS warns and leaves the price / share at 0, so these must be checked against
# real PSI output.

ordc_dual_key(name) = "RequirementConstraint__ReserveDemandCurve__ReserveUp__$(name)"
ordc_reserve_key(name) = "ActivePowerReserveVariable__ReserveDemandCurve__ReserveUp__$(name)"
ordc_storage_keys(name) = (
    "AncillaryServiceVariableDischarge__EnergyReservoirStorage__ReserveDemandCurve{ReserveUp}_$(name)",
    "AncillaryServiceVariableCharge__EnergyReservoirStorage__ReserveDemandCurve{ReserveUp}_$(name)",
)

"""All numeric values of a PSI result table (works for wide and long formats)."""
_numeric_values(df) =
    reduce(vcat, [Float64.(c) for c in eachcol(df) if eltype(c) <: Real]; init = Float64[])

"""Highest price on `service`'s ORDC curves over the forecast windows starting at `starts`."""
function ordc_max_price(service, starts)
    curves = vcat([PSY.get_time_series_values(PSY.Deterministic, service, "variable_cost"; start_time = s) for s in starts]...)
    return maximum(maximum(PSY.get_y_coords(c)) for c in curves)
end

_has_ordc_service(sys, service) =
    any(d -> service in PSY.get_services(d), PSY.get_components(PSY.EnergyReservoirStorage, sys))

"""
Check the ORDC results EMIS reads for every ReserveDemandCurve in `sys`:
result keys exist, prices (|dual| / base power, as in create_simulation) are finite and no
higher than the curve's top price, and reserves are provided.
`duals`/`variables` are Dict(key => DataFrame) of the results; `starts` the forecast windows solved.
"""
function check_ordc_results(sys, duals::AbstractDict, variables::AbstractDict, starts)
    base_power = PSY.get_base_power(sys)
    services = collect(PSY.get_components(PSY.ReserveDemandCurve{PSY.ReserveUp}, sys))
    @test !isempty(services)
    for service in services
        name = PSY.get_name(service)
        @testset "ORDC $(name) results" begin
            @test haskey(duals, ordc_dual_key(name))
            @test haskey(variables, ordc_reserve_key(name))
            if _has_ordc_service(sys, service)
                @test all(haskey(variables, k) for k in ordc_storage_keys(name))
            end
            if haskey(duals, ordc_dual_key(name))
                price = abs.(_numeric_values(duals[ordc_dual_key(name)])) ./ base_power
                @test !isempty(price) && all(isfinite, price)
                @test maximum(price) <= ordc_max_price(service, starts) * (1 + 1e-6) + 1e-6
            end
            if haskey(variables, ordc_reserve_key(name))
                provided = _numeric_values(variables[ordc_reserve_key(name)])
                @test all(>=(-1e-6), provided)
                @test sum(provided) > 0
            end
        end
    end
end
