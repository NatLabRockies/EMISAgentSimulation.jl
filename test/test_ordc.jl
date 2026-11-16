# Unit tests for the ORDC (operating reserve demand curve) pipeline. No solver needed.
#
#   construct_ordc                curve construction (net-load forecast error + outage distribution)
#   process_ordc_data_for_siip    curve CSV -> PSY.PiecewiseStepData (what Sienna/PSI clears)
#   create_ordc_market            curve CSV -> ReserveORDCMarket (what the investors' CEM clears)
#   make_ORDC_vectors             CEM segment sizes / gradients / prices
#   add_psy_ordc!                 ReserveDemandCurve service + Deterministic "variable_cost" forecast
#   create_{md,uc,ed}_template    ORDC service models in the operating-model templates
#   scale_voll                    realized price rescaling
#   calculate_realized_operating_profit   MD/UC/ED settlement of a reserve product
#
# Tests marked @test_broken document known problems in the current code (see the comment on
# each). They pass while the problem exists and turn into an "Unexpected Pass" error once it is
# fixed, at which point the @test_broken should become a @test.
#
# Run from REPL:
#   include("test/test_ordc.jl")

include(joinpath(@__DIR__, "includes.jl"))
include(joinpath(@__DIR__, "helpers.jl"))   # _dummy_finance

import Distributions
using Dates
using Statistics
import TimeSeries

ORDC_E = EMISAgentSimulation

# Committed RTS-GMLC inputs reused as fixtures (real season/timeblock/penalty definitions and a
# real system config for zone lookup).
const ORDC_RTS_SPEC_DIR =
    joinpath(@__DIR__, "..", "config", "example_systems", "rts_gmlc", "project_spec")
const ORDC_RTS_PENALTY_CSV =
    joinpath(ORDC_RTS_SPEC_DIR, "markets_data", "Mid_reserve_penalty", "Synchronous.csv")

# ── Independent helpers (deliberately not reusing the code under test) ──────────────────────

"""Parse a curve cell such as "[(0.0, 4000.0), (300.0, 4000.0)]" into (MW, \$/MWh) tuples."""
_ordc_parse_curve(s::AbstractString) =
    [(parse(Float64, m.captures[1]), parse(Float64, m.captures[2]))
     for m in eachmatch(r"\(\s*([-0-9.eE+]+)\s*,\s*([-0-9.eE+]+)\s*\)", s)]

_ordc_curve_string(points) = "[" * join(["($(x), $(y))" for (x, y) in points], ", ") * "]"

"""A case-style data dir (`<dir>/system_config/*.csv`, as load_system_config expects) built
from the RTS spec's system config files."""
function _ordc_sys_data_dir()
    dir = mktempdir()
    config = joinpath(dir, "system_config")
    mkpath(config)
    for f in ("system_config.csv", "zones.csv", "scenarios.csv", "technologies.csv", "devices_to_remove.csv")
        cp(joinpath(ORDC_RTS_SPEC_DIR, f), joinpath(config, f))
    end
    return dir
end

"""Months of a season such as "Dec-Feb" (wraps around the year end)."""
function _ordc_season_months(season::AbstractString)
    abbr = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    a, b = [findfirst(==(s), abbr) for s in split(season, "-")]
    return a <= b ? collect(a:b) : vcat(collect(1:b), collect(a:12))
end

"""Hours of a timeblock such as "2300-0200" (hour-ending 1..24, wraps around midnight)."""
function _ordc_block_hours(block::AbstractString)
    a, b = [parse(Int, s) ÷ 100 for s in split(block, "-")]
    return a <= b ? collect(a:b) : vcat(collect(1:b), collect(a:24))
end

_ordc_list(cell) = [strip(s, ['(', ')', ' ']) for s in split(string(cell), ";")]

# Season-dependent size of the real-time minus day-ahead load error, so that every season gets a
# different error distribution (and therefore a different curve width).
const ORDC_ERROR_AMPLITUDE = Dict(
    12 => 40.0, 1 => 40.0, 2 => 40.0,      # Dec-Feb
    3 => 120.0, 4 => 120.0, 5 => 120.0,    # Mar-May
    6 => 400.0, 7 => 400.0, 8 => 400.0,    # Jun-Aug
    9 => 220.0, 10 => 220.0, 11 => 220.0,  # Sep-Nov
)
_ordc_noise(m, h) = sin(1.7 * (24 * m + h)) + 0.5 * cos(0.9 * (24 * m + h))

"""
Write the inputs construct_ordc reads: markets_data/reserve_products.csv, the Mid penalty file,
and one day per month of day-ahead and real-time load (hourly in both, so the two files line up
row by row, as in the GS ERCOT runs). Single zone, no renewables.
"""
function _ordc_write_inputs(root::String; penalty_csv::String = ORDC_RTS_PENALTY_CSV)
    simulation_dir = joinpath(root, "simulation")
    timeseries_dir = joinpath(root, "timeseries")
    markets = joinpath(simulation_dir, "markets_data")
    mkpath(joinpath(markets, "Mid_reserve_penalty"))
    CSV.write(
        joinpath(markets, "reserve_products.csv"),
        DataFrame(
            all_products = ["Synchronous"],
            ordc_products = ["Synchronous"],
            da_products = ["Synchronous"],
            rt_products = ["Synchronous"],
        ),
    )
    cp(penalty_csv, joinpath(markets, "Mid_reserve_penalty", "Synchronous.csv"))

    da = DataFrame(Year = Int[], Month = Int[], Day = Int[], Period = Int[], load_zone_1 = Float64[])
    rt = similar(da, 0)
    for m in 1:12, h in 1:24
        load = 1000.0 + 10.0 * h
        push!(da, (2020, m, 1, h, load))
        push!(rt, (2020, m, 1, h, load + ORDC_ERROR_AMPLITUDE[m] * _ordc_noise(m, h)))
    end
    net_load = joinpath(timeseries_dir, "scenario_1", "sim_year_1", "Net Load Data")
    mkpath(net_load)
    CSV.write(joinpath(net_load, "load_n_vg_data.csv"), da)
    CSV.write(joinpath(net_load, "load_n_vg_data_rt.csv"), rt)
    return simulation_dir, timeseries_dir, da, rt
end

function _ordc_thermal(name::String, maxcap::Float64; FOR::Float64 = 0.05)
    tech = ThermalTech(
        "CT", "GAS", (min = 0.0, max = maxcap), nothing, nothing, nothing,
        0.0, [(0.0, 0.0)], "bus1", "zone_1", FOR, 48,
    )
    return ThermalGenEMIS{Existing}(name, tech, 1, 1, 30, 30, Product[], _dummy_finance())
end

"""The smallest Investor construct_ordc can use: it only reads get_existing(investor)."""
_ordc_investor(projects::Vector{Project}) = Investor(
    "investor1", "", projects, Symbol[], Float64[],
    MarketPrices(nothing, nothing, nothing, nothing, nothing),
    24, Dict{String, Dict{Int64, Vector{Float64}}}(), 4, true,
    Dict{String, Dict{Int64, Matrix{Int64}}}(), Perfect(Scenario[]), 1.0,
    (min = 1.0, max = 1.0), Dict{Tuple{String, String}, Vector{Float64}}(), 1, RiskNeutral(), 1,
)

const ORDC_UNIT_SIZES = [100.0, 300.0]   # MRR = largest unit x MRR_scale (1) = 300 MW

function _ordc_run_construct(root::String; curved::Bool, representative_periods = Dict(1 => 1, 3 => 1))
    simulation_dir, timeseries_dir, da, rt = _ordc_write_inputs(root)
    investors = [_ordc_investor(Project[_ordc_thermal("th_$(i)", s) for (i, s) in enumerate(ORDC_UNIT_SIZES)])]
    ORDC_E.construct_ordc(
        PSY.System(100.0), simulation_dir, "scenario_1", 1, investors, 1,
        representative_periods, 24, curved, "Convolution", "Mid", timeseries_dir,
    )
    reserves = joinpath(timeseries_dir, "scenario_1", "sim_year_1", "Reserves")
    return (; simulation_dir, timeseries_dir, reserves, da, rt)
end

# ── construct_ordc ──────────────────────────────────────────────────────────────────────────

@testset "ORDC construction (construct_ordc)" begin
    penalty_df = CSV.read(ORDC_RTS_PENALTY_CSV, DataFrame)
    penalty = Float64(penalty_df[1, "penalty"])
    step_csv = Float64(penalty_df[1, "stepsize (MW)"])
    mrr = maximum(ORDC_UNIT_SIZES) * penalty_df[1, "MRR_scale"]
    seasons = _ordc_list(penalty_df[1, "seasons"])
    blocks = _ordc_list(penalty_df[1, "timeblocks"])

    @testset "RTS seasons and timeblocks cover every month and hour exactly once" begin
        month_hits = zeros(Int, 12)
        for s in seasons, m in _ordc_season_months(s)
            month_hits[m] += 1
        end
        hour_hits = zeros(Int, 24)
        for b in blocks, h in _ordc_block_hours(b)
            hour_hits[h] += 1
        end
        @test all(==(1), month_hits)
        @test all(==(1), hour_hits)
        @test any(b -> first(_ordc_block_hours(b)) == 1 && last(_ordc_block_hours(b)) == 24, blocks)  # a block wraps midnight
    end

    @testset "flat ORDC (ordc_curved = false)" begin
        out = _ordc_run_construct(mktempdir(); curved = false)
        da_curves = CSV.read(joinpath(out.reserves, "Synchronous.csv"), DataFrame)
        @test DataFrames.nrow(da_curves) == DataFrames.nrow(out.da)
        @test all(_ordc_parse_curve(c) == [(0.0, penalty), (mrr, penalty)] for c in da_curves.Synchronous)
    end

    out = _ordc_run_construct(mktempdir(); curved = true)
    da_curves = CSV.read(joinpath(out.reserves, "Synchronous.csv"), DataFrame)
    rt_curves = CSV.read(joinpath(out.reserves, "Synchronous_REAL_TIME.csv"), DataFrame)
    rep_curves = CSV.read(joinpath(out.reserves, "rep_Synchronous.csv"), DataFrame)
    curves = [_ordc_parse_curve(c) for c in da_curves.Synchronous]

    @testset "curved ORDC: one curve per hour, 2 + 10 points" begin
        @test DataFrames.nrow(da_curves) == DataFrames.nrow(out.da)
        @test da_curves.Month == out.da.Month && da_curves.Period == out.da.Period
        # PSI keeps time-varying curves in parameter arrays sized at build time, so every hour
        # must have the same number of points.
        @test all(length(c) == 12 for c in curves)
    end

    @testset "curved ORDC: shape" begin
        @test all(c[1] == (0.0, penalty) && c[2] == (mrr, penalty) for c in curves)
        @test all(all(diff(first.(c)) .> 0) for c in curves)          # MW strictly increasing
        @test all(all(diff(last.(c)) .<= 1e-9) for c in curves)       # price never increases
        @test all(all(0 .<= last.(c) .<= penalty) for c in curves)
    end

    # Recompute each (season, timeblock) block's distribution independently from the inputs.
    unavail = ORDC_E.spconv(round.(Int, ORDC_UNIT_SIZES), fill(0.05, length(ORDC_UNIT_SIZES)))
    unavail_mean = Distributions.mean(unavail)
    unavail_std = sqrt(Distributions.var(unavail))
    block_results = NamedTuple[]
    for s in seasons, b in blocks
        months = _ordc_season_months(s)
        hours = _ordc_block_hours(b)
        rows = findall(r -> r.Month in months && r.Period in hours, eachrow(out.da))
        err = out.rt.load_zone_1[rows] .- out.da.load_zone_1[rows]
        mu = mean(err) + unavail_mean
        sigma = sqrt(std(err)^2 + unavail_std^2)
        dist = Distributions.Normal(mu, sigma)
        meanload = mean(out.da.load_zone_1[rows])
        max_error = round(ceil((mu + 3 * sigma) / step_csv)) * step_csv   # rounding with the CSV step size
        max_error < min(sigma, 0.1 * meanload) && (max_error = min(sigma, 0.1 * meanload))
        push!(block_results, (; season = s, block = b, rows, dist, max_error))
    end

    @testset "curved ORDC: identical curve within each season x timeblock" begin
        @test all(allequal(curves[r.rows]) for r in block_results)
        @test length(unique(curves)) > 1   # seasons differ, so curves differ
    end

    @testset "curved ORDC: tail prices = (1 - CDF(k * step)) * penalty" begin
        for r in block_results, row in r.rows
            c = curves[row]
            step = c[3][1] - c[2][1]
            @test all(isapprox(c[k + 2][1], mrr + k * step; rtol = 1e-9) for k in 1:10)
            @test all(
                isapprox(c[k + 2][2], (1 - Distributions.cdf(r.dist, k * step)) * penalty; rtol = 1e-6, atol = 1e-6)
                for k in 1:10
            )
        end
    end

    @testset "curved ORDC: tail width uses the CSV step size in every block" begin
        widths_match = [isapprox(10 * (curves[first(r.rows)][3][1] - mrr), r.max_error; rtol = 1e-9) for r in block_results]
        @test any(widths_match)   # the first block processed uses the CSV step size
        # Known bug: construct_ordc reassigns `step_size = maximum_error / n_steps` inside the
        # season x timeblock loop (ordc_construction.jl:266), so every later block rounds its
        # maximum error to the previous block's step instead of the CSV "stepsize (MW)".
        @test_broken all(widths_match)
    end

    @testset "real-time and representative-period curve files" begin
        # Hourly real-time data here, so every real-time row carries the same curve as its hour.
        @test DataFrames.nrow(rt_curves) == DataFrames.nrow(da_curves)
        @test rt_curves.Synchronous == da_curves.Synchronous
        # rep_period_interval = 24 -> representative period = day of the file; days 1 and 3 kept.
        @test DataFrames.nrow(rep_curves) == 48
        @test rep_curves.Month == vcat(fill(1, 24), fill(3, 24))
        @test rep_curves.Synchronous == vcat(da_curves.Synchronous[1:24], da_curves.Synchronous[49:72])
    end
end

# ── The two curve parsers must read the same curve ──────────────────────────────────────────

@testset "ORDC curve parsers (PSI step curve vs CEM market)" begin
    out = _ordc_run_construct(mktempdir(); curved = true)
    raw = ORDC_E.read_data(joinpath(out.reserves, "Synchronous.csv"))[:, "Synchronous"]
    rep_raw = ORDC_E.read_data(joinpath(out.reserves, "rep_Synchronous.csv"))[:, "Synchronous"]
    parameters = ORDC_E.read_data(ORDC_RTS_PENALTY_CSV)
    sys_data_dir = _ordc_sys_data_dir()

    for column in (raw, rep_raw, Vector{String}(raw))   # pooled and plain string columns
        expected = [_ordc_parse_curve(c) for c in column]
        step_curves = ORDC_E.process_ordc_data_for_siip(column)
        market = ORDC_E.create_ordc_market(column, parameters, ["th_1", "th_2"], sys_data_dir)

        @test length(step_curves) == length(column)
        # PSI: breakpoints = curve MW; one price per segment = the price at the segment's right end.
        @test all(PSY.get_x_coords(step_curves[t]) == first.(expected[t]) for t in eachindex(column))
        @test all(PSY.get_y_coords(step_curves[t]) == last.(expected[t])[2:end] for t in eachindex(column))
        # CEM: the same breakpoints and the price at every breakpoint.
        @test all(market.break_points[t] == first.(expected[t]) for t in eachindex(column))
        @test all(market.price_points[t] == last.(expected[t]) for t in eachindex(column))
        @test market.zones == ["zone_1", "zone_2", "zone_3"]
        @test market.stepped == false   # RTS Mid penalty file
    end
end

# ── CEM segment data ────────────────────────────────────────────────────────────────────────

@testset "CEM ORDC segments (make_ORDC_vectors)" begin
    x = [0.0, 300.0, 400.0, 500.0]
    p = [4000.0, 4000.0, 3000.0, 1000.0]
    _market(stepped) = ORDC_E.ReserveORDCMarket(
        AxisArrays.AxisArray([copy(x), copy(x)], 1:2),
        AxisArrays.AxisArray([copy(p), copy(p)], 1:2),
        ["zone_1"], ["th_1"], stepped,
    )

    @testset "piecewise-linear (stepped = false, as in the RTS inputs)" begin
        market = _market(false)
        seg_size, grad, price, nseg = ORDC_E.make_ORDC_vectors([Dict("Synchronous" => market)])
        @test nseg["Synchronous"][1, 1] == 3
        @test seg_size["Synchronous"][1, 2] == [300.0, 100.0, 100.0]
        @test grad["Synchronous"][1, 2] == [0.0, -10.0, -20.0]
        @test price["Synchronous"][1, 2] == p
    end

    @testset "stepped (stepped = true): each segment takes its right-end price" begin
        market = _market(true)
        seg_size, grad, price, nseg = ORDC_E.make_ORDC_vectors([Dict("Synchronous" => market)])
        @test seg_size["Synchronous"][1, 1] == [300.0, 100.0, 100.0]
        @test grad["Synchronous"][1, 1] == zeros(3)
        @test price["Synchronous"][1, 1][1:3] == [4000.0, 3000.0, 1000.0]
        # Known issue: price_points[product][p, t] is the market's own vector (no copy), and the
        # stepped branch shifts it in place, so the ReserveORDCMarket is modified and a second
        # call shifts the prices again (market_utils.jl, make_ORDC_vectors).
        @test_broken market.price_points[1] == p
        _, _, price_again, _ = ORDC_E.make_ORDC_vectors([Dict("Synchronous" => market)])
        @test_broken price_again["Synchronous"][1, 1][1:3] == [4000.0, 3000.0, 1000.0]
    end
end

# ── ReserveDemandCurve service and its forecast ─────────────────────────────────────────────

"""Small copper-plate system: one bus, one thermal unit, one wind unit, one load, with
Deterministic forecasts (48 h horizon, 24 h interval) over 8784 hours, like the UC systems."""
function _ordc_psy_system()
    sys = PSY.System(100.0)
    bus = PSY.ACBus(;
        number = 1, name = "bus1", available = true, bustype = PSY.ACBusTypes.REF,
        angle = 0.0, magnitude = 1.0, voltage_limits = (min = 0.9, max = 1.1), base_voltage = 230.0,
    )
    PSY.add_component!(sys, bus)
    PSY.add_component!(sys, PSY.ThermalStandard(;
        name = "th_1", available = true, status = true, bus = bus, active_power = 0.0,
        reactive_power = 0.0, rating = 1.0, active_power_limits = (min = 0.0, max = 1.0),
        reactive_power_limits = nothing, ramp_limits = nothing,
        operation_cost = PSY.ThermalGenerationCost(nothing), base_power = 100.0,
    ))
    PSY.add_component!(sys, PSY.RenewableDispatch(;
        name = "wind_1", available = true, bus = bus, active_power = 0.0, reactive_power = 0.0,
        rating = 1.0, prime_mover_type = PSY.PrimeMovers.WT, reactive_power_limits = nothing,
        power_factor = 1.0, operation_cost = PSY.RenewableGenerationCost(nothing), base_power = 100.0,
    ))
    load = PSY.PowerLoad(;
        name = "load_1", available = true, bus = bus, active_power = 1.0, reactive_power = 0.0,
        base_power = 100.0, max_active_power = 1.0, max_reactive_power = 0.0,
    )
    PSY.add_component!(sys, load)
    stamps = DateTime(2020, 1, 1) .+ Hour.(0:8783)
    PSY.add_time_series!(sys, load, PSY.SingleTimeSeries("max_active_power", TimeSeries.TimeArray(stamps, ones(8784))))
    PSY.transform_single_time_series!(sys, Hour(48), Hour(24))
    return sys
end

"""A distinct 4-point curve for every hour of the year, so window alignment can be checked."""
_ordc_hour_curve(i) = (m = 100.0 + (i % 97); [(0.0, 4000.0), (m, 4000.0), (m + 50.0, 2000.0), (m + 100.0, 500.0)])

function _ordc_write_hourly_curves(timeseries_dir)
    dir = joinpath(timeseries_dir, "scenario_1", "sim_year_1", "Reserves")
    mkpath(dir)
    stamps = DateTime(2020, 1, 1) .+ Hour.(0:8759)
    CSV.write(joinpath(dir, "Synchronous.csv"), DataFrame(
        Year = year.(stamps), Month = month.(stamps), Day = day.(stamps), Period = hour.(stamps) .+ 1,
        Synchronous = [_ordc_curve_string(_ordc_hour_curve(i)) for i in 1:8760],
    ))
end

@testset "ORDC service and forecast (add_psy_ordc!)" begin
    root = mktempdir()
    simulation_dir, timeseries_dir, _, _ = _ordc_write_inputs(root)
    _ordc_write_hourly_curves(timeseries_dir)
    sys = _ordc_psy_system()
    ORDC_E.add_psy_ordc!(
        simulation_dir, Dict(:Synchronous => true), sys, "UC", "scenario_1", 1, 60, 5, "Mid", timeseries_dir,
    )

    reserve = PSY.get_component(PSY.ReserveDemandCurve{PSY.ReserveUp}, sys, "Synchronous")
    @testset "service exists and is attached to eligible devices" begin
        @test reserve !== nothing
        @test reserve in PSY.get_services(PSY.get_component(PSY.ThermalStandard, sys, "th_1"))
        # The RTS penalty file lists "Wind" among the eligible categories -> RenewableDispatch.
        @test reserve in PSY.get_services(PSY.get_component(PSY.RenewableDispatch, sys, "wind_1"))
    end

    @testset "variable_cost forecast windows follow the curve file (with year-end wrap)" begin
        initial = PSY.get_forecast_initial_timestamp(sys)
        count = PSY.get_forecast_window_count(sys)
        @test count == 365
        for w in (1, 2, 200, count)
            values = PSY.get_time_series_values(
                PSY.Deterministic, reserve, "variable_cost";
                start_time = initial + Hour(24 * (w - 1)), len = 48,
            )
            for (k, curve) in enumerate(values)
                hour_of_year = 24 * (w - 1) + k
                hour_of_year > 8760 && (hour_of_year -= 8760)   # padded from the start of the year
                expected = _ordc_hour_curve(hour_of_year)
                @test PSY.get_x_coords(curve) == first.(expected)
                @test PSY.get_y_coords(curve) == last.(expected)[2:end]
            end
        end
    end

    @testset "wind is not attached when Wind is not an eligible category" begin
        penalty = CSV.read(ORDC_RTS_PENALTY_CSV, DataFrame)
        penalty[!, "eligible categories"] .= "(Gas CT,Gas CC)"
        thermal_only = joinpath(mktempdir(), "Synchronous.csv")
        CSV.write(thermal_only, penalty)
        root2 = mktempdir()
        simulation_dir2, timeseries_dir2, _, _ = _ordc_write_inputs(root2; penalty_csv = thermal_only)
        _ordc_write_hourly_curves(timeseries_dir2)
        sys2 = _ordc_psy_system()
        ORDC_E.add_psy_ordc!(
            simulation_dir2, Dict(:Synchronous => true), sys2, "UC", "scenario_1", 1, 60, 5, "Mid", timeseries_dir2,
        )
        reserve2 = PSY.get_component(PSY.ReserveDemandCurve{PSY.ReserveUp}, sys2, "Synchronous")
        @test reserve2 in PSY.get_services(PSY.get_component(PSY.ThermalStandard, sys2, "th_1"))
        @test !(reserve2 in PSY.get_services(PSY.get_component(PSY.RenewableDispatch, sys2, "wind_1")))
    end

    @testset "product not included in markets -> no service" begin
        root3 = mktempdir()
        simulation_dir3, timeseries_dir3, _, _ = _ordc_write_inputs(root3)
        _ordc_write_hourly_curves(timeseries_dir3)
        sys3 = _ordc_psy_system()
        ORDC_E.add_psy_ordc!(
            simulation_dir3, Dict(:Synchronous => false), sys3, "UC", "scenario_1", 1, 60, 5, "Mid", timeseries_dir3,
        )
        @test isempty(PSY.get_components(PSY.ReserveDemandCurve{PSY.ReserveUp}, sys3))
    end
end

# ── Operating-model templates ───────────────────────────────────────────────────────────────

_ordc_has_service(template, type, formulation, name) = any(
    m -> m isa PSI.ServiceModel{type, formulation} && PSI.get_service_name(m) == name,
    values(PSI.get_service_models(template)),
)

@testset "ORDC service models in the MD/UC/ED templates" begin
    ordc = (PSY.ReserveDemandCurve{PSY.ReserveUp}, PSI.StepwiseCostReserve)
    for name in ("Synchronous", "Primary")
        @test _ordc_has_service(ORDC_E.create_md_template(), ordc..., name)
        @test _ordc_has_service(ORDC_E.create_uc_template(), ordc..., name)
        # ED gets ORDC only when the system has an Inertia service (create_ed_template).
        @test !_ordc_has_service(ORDC_E.create_ed_template([]), ordc..., name)
        @test _ordc_has_service(ORDC_E.create_ed_template([:inertia]), ordc..., name)
    end
    # Regulation stays in every template.
    for template in (ORDC_E.create_md_template(), ORDC_E.create_uc_template(), ORDC_E.create_ed_template([]))
        @test _ordc_has_service(template, PSY.VariableReserve{PSY.ReserveUp}, PSI.RangeReserve, "Reg_Up")
        @test _ordc_has_service(template, PSY.VariableReserve{PSY.ReserveDown}, PSI.RangeReserve, "Reg_Down")
    end
end

# ── Realized price scaling and settlement ───────────────────────────────────────────────────

@testset "scale_voll on ORDC prices" begin
    prices = [500.0 989.0 990.0 4000.0]   # increasing prices along an ORDC
    hourly = copy(prices)
    ORDC_E.scale_voll(hourly, 60)
    @test hourly == prices   # MD and UC (60-minute): ORDC prices are unchanged

    five_min = copy(prices)
    ORDC_E.scale_voll(five_min, 5)
    @test five_min[1:2] == prices[1:2]
    @test five_min[3:4] ≈ prices[3:4] .* 5 / 60
    # Known issue: the >= 990 threshold is meant for scarcity (VOLL) prices, but it also catches
    # ORDC prices between 990 and the penalty (4,000 for Mid). In a 5-minute stage the curve's
    # price order is broken: 989 stays 989 while 4,000 becomes 333. Only matters when ED models
    # ORDC (Inertia cases).
    @test_broken issorted(vec(five_min))
end

@testset "reserve settlement across MD/UC/ED (calculate_realized_operating_profit)" begin
    weights = Dict{String, Dict{Int64, Vector{Float64}}}()
    md, uc, ed = [10.0 20.0], [30.0 40.0], [50.0 0.0]          # $/MWh
    out_md, out_uc, out_ed = [1.0 2.0], [3.0 2.0], [4.0 1.0]  # MW
    # md*out_md + uc*(out_uc - out_md) + ed*(out_ed - out_uc) - mc*out_ed, by hand:
    # hour 1: 10 + 60 + 50 - 2.0 = 118.0 ; hour 2: 40 + 0 + 0 - 0.5 = 39.5
    @test ORDC_E.calculate_realized_operating_profit(
        copy(md), copy(uc), copy(ed), 0.5, copy(out_md), copy(out_uc), copy(out_ed), weights,
    ) ≈ 157.5

    # RTS case (no Inertia): ED has no ORDC, so its ORDC price and reserve share are both 0 and
    # only the MD and UC terms remain. Note the marginal reserve cost is charged on the ED
    # quantity only, so it drops out too.
    @test ORDC_E.calculate_realized_operating_profit(
        copy(md), copy(uc), zeros(1, 2), 0.5, copy(out_md), copy(out_uc), zeros(1, 2), weights,
    ) ≈ 10 + 60 + 40 + 0

    # NaNs (e.g. from a missing result) are treated as 0.
    @test ORDC_E.calculate_realized_operating_profit(
        [NaN 0.0], [0.0 0.0], [0.0 0.0], 0.0, [1.0 0.0], [1.0 0.0], [1.0 0.0], weights,
    ) == 0.0
end
