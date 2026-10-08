# Post-run checks for a completed EMIS run; works on any results directory without re-running anything.
#
# Usage:
#   julia --project=. test/integration_test/check_run.jl --results-dir DIR [--elapsed-seconds N]
#       [--price-cap 10000] [--peak-rtol 0.02] [--max-lost-load-share 0.01] [--min-capacity-margin 1.0]
#   julia --project=. test/integration_test/check_run.jl --project-dir DIR --case-name NAME --run-name RUN   # legacy form
#
# Hard checks (missing files, load peaks, NaN/negative prices or RA metrics, no capacity) set a nonzero exit code;
# warn checks and info lines only report. Writes `run_summary.csv` and `run_checks.csv` into the results directory.

using CSV
using DataFrames
using Dates
using HDF5
using Logging
using PowerSystems
using Printf
using Statistics
using EMISAgentSimulation

const PSY = PowerSystems
const EAS = EMISAgentSimulation

# ---------------------------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------------------------

function arg_value(flag::AbstractString, default = nothing)
    index = findfirst(==(flag), ARGS)
    if index === nothing
        default === nothing && error("Missing required argument $(flag)")
        return default
    end
    index == length(ARGS) && error("Missing value after $(flag)")
    return ARGS[index + 1]
end

read_key_values(path) =
    Dict(strip(string(r[1])) => strip(string(r[2])) for r in CSV.File(path; stringtype = String))

function resolve_results_dir()
    "--results-dir" in ARGS && return String(arg_value("--results-dir"))
    metadata = read_key_values(joinpath(arg_value("--project-dir"), "project_metadata.csv"))
    return joinpath(metadata["runs_dir"], arg_value("--case-name"), "Results", arg_value("--run-name"))
end

# ---------------------------------------------------------------------------------------------
# Check bookkeeping
# ---------------------------------------------------------------------------------------------

struct CheckResult
    name::String
    level::String   # hard, warn or info
    status::String  # pass, FAIL, WARN or info
    detail::String
end

const CHECKS = CheckResult[]

function record!(name::AbstractString, level::AbstractString, ok::Bool, detail::AbstractString = "")
    status = level == "info" ? "info" : ok ? "pass" : level == "hard" ? "FAIL" : "WARN"
    push!(CHECKS, CheckResult(name, level, status, detail))
    println(rpad(status, 5), " ", name, isempty(detail) ? "" : " - " * detail)
    return ok
end

hard!(name, ok::Bool, detail = "") = record!(name, "hard", ok, detail)
soft!(name, ok::Bool, detail = "") = record!(name, "warn", ok, detail)
note!(name, detail) = record!(name, "info", true, detail)

"""Run `f`; an exception becomes a failed hard check so the remaining checks still execute."""
function guarded(f, name::AbstractString)
    try
        f()
    catch err
        hard!(name, false, "error: " * first(sprint(showerror, err), 300))
    end
    return nothing
end

fmt(x::Real; digits::Int = 2) = @sprintf("%.*f", digits, x)
series(values::AbstractVector{<:Real}; digits::Int = 1) =
    join(["y$(y)=$(fmt(v; digits))" for (y, v) in enumerate(values)], " ")

# ---------------------------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------------------------

function checkpoint_years(results_dir::AbstractString)
    years = Int[]
    for file in readdir(results_dir)
        m = match(r"^simulation_data_year_(\d+)\.h5$", file)
        m === nothing || push!(years, parse(Int, m.captures[1]))
    end
    return sort(years)
end

quiet(f) = Logging.with_logger(f, Logging.NullLogger())

load_market(results_dir, year) =
    quiet(() -> EAS.load_realized_market_data(joinpath(results_dir, "realized_market_data", "year_$(year).h5")))

"""Peak (MW) of the system-wide canonical day-ahead load file, or `nothing` when the file is absent."""
function canonical_peak_mw(timeseries_dir, scenario, sim_year)
    path = joinpath(timeseries_dir, scenario, "sim_year_$(sim_year)", "Load", "DAY_AHEAD_regional_Load.csv")
    isfile(path) || return nothing
    load = DataFrame(CSV.File(path))
    return maximum(sum(eachcol(load[:, 5:end])))
end

"""Peak (MW) of the realized hourly system load in an MD system (first `interval` hours of each window)."""
function md_system_peak_mw(md_path)
    return mktempdir() do tmp
        sys = PSY.System(md_path; runchecks = false, time_series_directory = tmp)
        PSY.set_units_base_system!(sys, PSY.UnitSystem.NATURAL_UNITS)
        interval_hours = Dates.Millisecond(PSY.get_forecast_interval(sys)).value ÷ 3_600_000
        initial_times = collect(PSY.get_forecast_initial_times(sys))
        hourly = zeros(length(initial_times) * interval_hours)
        for load in PSY.get_components(EAS.PSY_LOADS, sys)
            for (w, start) in enumerate(initial_times)
                values = PSY.get_time_series_values(
                    PSY.Deterministic, load, "max_active_power"; start_time = start, len = interval_hours,
                )
                hourly[((w - 1) * interval_hours + 1):(w * interval_hours)] .+= values
            end
        end
        maximum(hourly)
    end
end

# ---------------------------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------------------------

function check_files(ctx)
    missing_files = String[]
    expected = 0
    function expect(path)
        expected += 1
        ispath(path) || push!(missing_files, relpath(path, ctx.results_dir))
    end
    r(parts...) = joinpath(ctx.results_dir, parts...)

    expect(r("timeseries_data_files"))
    expect(r("clean_energy_percentage.h5"))
    for y in 1:ctx.simulation_years
        expect(r("simulation_data_year_$(y).h5"))
        expect(r("realized_market_data", "year_$(y).h5"))
        expect(r("clean_energy_percentage_year$(y).h5"))
    end
    for scenario in ctx.scenarios, y in 1:ctx.simulation_years
        expect(r("shortfall_data_$(scenario)_year$(y).h5"))
        for suffix in (".json", "_metadata.json", "_time_series_storage.h5")
            expect(r("sys_PRAS_$(scenario)_year$(y)$(suffix)"))
        end
    end
    for kind in ("MD", "UC", "ED"), y in 1:ctx.total_horizon, suffix in (".json", "_metadata.json", "_time_series_storage.h5")
        expect(r("sys_$(kind)_year$(y)$(suffix)"))
    end
    for investor in EAS.get_investors(ctx.sim)
        expect(r(EAS.get_name(investor)))
    end
    detail = isempty(missing_files) ? "all $(expected) expected files present" :
        "missing $(length(missing_files)) of $(expected): " * join(first(missing_files, 8), ", ") *
        (length(missing_files) > 8 ? ", ..." : "")
    hard!("result files written", isempty(missing_files), detail)

    # Derating files are written one year ahead and may legitimately be absent for the last years.
    missing_derating = [
        "$(s)/year_$(y)" for s in ctx.scenarios for y in 1:ctx.simulation_years
        if !isfile(r("derating_data", s, "derating_data_year_$(y).h5"))
    ]
    soft!("derating files written", isempty(missing_derating),
        isempty(missing_derating) ? "present for every simulated year" : "missing: " * join(missing_derating, ", "))
end

function check_load(ctx)
    peaks = Float64[]
    for y in 1:ctx.simulation_years
        name = "load peak year $(y)"
        guarded(name) do
            md_path = joinpath(ctx.results_dir, "sys_MD_year$(y).json")
            peak = md_system_peak_mw(md_path)
            push!(peaks, peak)
            expected = canonical_peak_mw(joinpath(ctx.results_dir, "timeseries_data_files"), ctx.pcm_scenario, y)
            if expected === nothing
                note!(name, "no canonical load file in results; MD peak $(fmt(peak)) MW (comparison skipped)")
            else
                hard!(name, isapprox(peak, expected; rtol = ctx.peak_rtol),
                    "MD system peak $(fmt(peak)) MW vs timeseries peak $(fmt(expected)) MW")
            end
            stored = get(get(ctx.sim.peak_load, ctx.pcm_scenario, Dict{Int, Float64}()), y, nothing)
            stored === nothing || soft!("checkpoint peak_load year $(y)", isapprox(peak, stored; rtol = ctx.peak_rtol),
                "checkpoint $(fmt(stored)) MW vs MD system peak $(fmt(peak)) MW")
        end
    end
    length(peaks) >= 2 && note!("peak load growth", series(peaks ./ first(peaks); digits = 4) * " (relative to year 1)")
    return peaks
end

"""Split `x` into `nbins` equal-width bins over its own range; returns counts and edges."""
function bin_counts(x::AbstractVector{<:Real}, nbins::Int = 4)
    lo, hi = extrema(x)
    width = (hi - lo) / nbins
    counts = zeros(Int, nbins)
    for v in x
        counts[width == 0 ? 1 : clamp(floor(Int, (v - lo) / width) + 1, 1, nbins)] += 1
    end
    return counts, [lo + i * width for i in 0:nbins]
end

function check_market(ctx)
    stages = (
        (; label = "DA/UC", price = "energy_price_uc", voll = "energy_voll_uc", resolution = ctx.da_resolution),
        (; label = "RT/ED", price = "energy_price_ed", voll = "energy_voll", resolution = ctx.rt_resolution),
        (; label = "MD", price = "energy_price_md", voll = "energy_voll_md", resolution = ctx.da_resolution),
    )
    for y in 1:ctx.simulation_years
        guarded("market data year $(y)") do
            data = load_market(ctx.results_dir, y)
            for stage in stages
                prices = dropdims(data[stage.price].data; dims = 2)  # zones x intervals
                label = "$(stage.label) year $(y)"
                if stage.label == "MD" && all(iszero, prices)
                    note!("energy price $(label)", "all zero (market-design stage not run); skipped")
                    continue
                end
                hard!("energy price valid $(label)", all(isfinite, prices) && minimum(prices) >= 0,
                    "min $(fmt(minimum(prices))) max $(fmt(maximum(prices)))")

                average = vec(Statistics.mean(prices; dims = 1))
                counts, edges = bin_counts(average)
                bins = join(
                    ["[$(fmt(edges[i]; digits = 1)),$(fmt(edges[i + 1]; digits = 1))]: $(counts[i]) " *
                     "($(fmt(100 * counts[i] / length(average); digits = 1))%)" for i in 1:4],
                    " | ",
                )
                note!("zone-average energy price $(label)",
                    "mean $(fmt(Statistics.mean(average))) over $(length(average)) intervals; 4 bins: $(bins)")

                # Realized slack ("voll") is stored per-unit on BASE_POWER.
                unserved_mw = vec(sum(dropdims(data[stage.voll].data; dims = 2); dims = 1)) .* EAS.BASE_POWER
                unserved_share = sum(unserved_mw .> 1e-6) / length(unserved_mw)
                # The simulation rescales real-time prices at the cap by resolution/60 (scale_voll), so scale the cap too.
                cap_effective = stage.label == "RT/ED" ? ctx.price_cap * min(stage.resolution, 60) / 60 : ctx.price_cap
                zone_cap_share = vec(Statistics.mean(prices .>= 0.99 * cap_effective; dims = 2))
                cap_share = sum(vec(any(prices .>= 0.99 * cap_effective; dims = 1))) / size(prices, 2)
                by_zone = join(["$(z)=$(fmt(100 * s; digits = 1))%" for (z, s) in zip(ctx.zones, zone_cap_share)], " ")
                note!("lost load $(label)",
                    "price at cap (>= $(fmt(cap_effective))) in any zone for $(fmt(100 * cap_share; digits = 2))% of intervals " *
                    "(by zone: $(by_zone)); unserved energy in $(fmt(100 * unserved_share; digits = 2))% of intervals, " *
                    "$(fmt(sum(unserved_mw) * stage.resolution / 60)) MWh")
                limit = "limit $(fmt(100 * ctx.max_lost_load_share; digits = 2))%"
                soft!("lost-load share $(label)", unserved_share <= ctx.max_lost_load_share,
                    "$(fmt(100 * unserved_share; digits = 2))% of intervals ($(limit))")
                soft!("price-cap share $(label)", cap_share <= ctx.max_lost_load_share,
                    "$(fmt(100 * cap_share; digits = 2))% of intervals ($(limit))" *
                    (cap_share > 0 && unserved_share == 0 ? "; prices at cap without any unserved energy, check price/slack extraction" : ""))
            end
        end
    end
end

function pras_totals(path)
    h5open(path, "r") do f
        g = f["shortfall"]
        return (eue = sum(read(g, "shortfall_mean")), lole = read(g, "eventperiod_mean"), hours = length(read(g, "timestamps")))
    end
end

function check_resource_adequacy(ctx)
    for (scenario, ra) in sort(collect(EAS.get_resource_adequacy(ctx.sim)); by = first)
        metrics = EAS.get_metrics(ra)
        for metric in ("LOLE", "NEUE")
            name = "RA $(metric) $(scenario)"
            guarded(name) do
                values = [y <= length(metrics) ? get(metrics[y], metric, NaN) : NaN for y in 1:ctx.simulation_years]
                hard!(name, all(isfinite, values) && all(>=(0), values), series(values; digits = 3))
                if length(values) >= 2 && isfinite(values[1]) && values[1] > 0
                    note!("RA $(metric) trend $(scenario)",
                        join(["y$(y)/y1=$(fmt(values[y] / values[1]; digits = 3))" for y in 2:length(values)], " "))
                end
            end
        end
        for y in 1:ctx.simulation_years
            name = "PRAS shortfall $(scenario) year $(y)"
            guarded(name) do
                totals = pras_totals(joinpath(ctx.results_dir, "shortfall_data_$(scenario)_year$(y).h5"))
                hard!(name, isfinite(totals.eue) && totals.eue >= 0 && isfinite(totals.lole) && totals.lole >= 0,
                    "EUE $(fmt(totals.eue)) MWh and LOLE $(fmt(totals.lole)) events over $(totals.hours) h")
            end
        end
    end
end

"""Derating factor for a project, or `nothing` when the derating table has no matching column."""
function derating_factor(derating::DataFrame, type::AbstractString, zone::AbstractString)
    for column in ("existing_$(type)_$(zone)", type, type in ("BA", "LDES") ? "STOR_4" : type)
        column in names(derating) && return Float64(derating[1, column])
    end
    return nothing
end

function load_derating(ctx, year)
    for candidate in (year, 1)  # later years fall back to year 1 when a year's file is missing
        path = joinpath(ctx.results_dir, "derating_data", ctx.pcm_scenario, "derating_data_year_$(candidate).h5")
        isfile(path) && return quiet(() -> EAS.load_derating_factors(path))
    end
    return nothing
end

function check_capacity(ctx)
    guarded("installed capacity") do
        n_years = ctx.simulation_years
        rows = map(EAS.get_decidedprojects(ctx.sim)) do p
            tech = EAS.get_tech(p)
            (; type = String(EAS.get_type(tech)), zone = String(EAS.get_zone(tech)), mw = Float64(EAS.get_maxcap(p)),
                built = Int(EAS.get_construction_year(p)), retire = Int(EAS.get_retirement_year(p)),
                retired = p isa EAS.Project{EAS.Retired})
        end
        hard!("project capacities and years valid", all(r -> isfinite(r.mw) && r.mw >= 0 && r.built >= 1, rows),
            "$(length(rows)) decided projects")

        types = sort(unique(r.type for r in rows))
        installed = Dict(t => zeros(n_years) for t in types)
        built = Dict(t => zeros(n_years) for t in types)
        retired = Dict(t => zeros(n_years) for t in types)
        derated = zeros(n_years)
        for y in 1:n_years
            derating = load_derating(ctx, y)
            for r in rows
                if r.built <= y < r.retire
                    installed[r.type][y] += r.mw
                    factor = derating === nothing ? nothing : derating_factor(derating, r.type, r.zone)
                    derated[y] += r.mw * something(factor, 1.0)
                end
                r.built == y && y > 1 && (built[r.type][y] += r.mw)  # year-1 builds are the initial fleet
                r.retired && r.retire == y && (retired[r.type][y] += r.mw)
            end
        end
        total = [sum(installed[t][y] for t in types) for y in 1:n_years]
        hard!("installed capacity positive", all(>(0), total), "total MW: " * series(total; digits = 0))
        for t in types
            note!("capacity $(t) (MW)", "installed $(series(installed[t]; digits = 0)) | " *
                "new $(series(built[t]; digits = 0)) | retired $(series(retired[t]; digits = 0))")
        end
        note!("capacity built / retired, all technologies (MW)",
            "new $(series([sum(built[t][y] for t in types) for y in 1:n_years]; digits = 0)) | " *
            "retired $(series([sum(retired[t][y] for t in types) for y in 1:n_years]; digits = 0))")

        peaks = [get(get(ctx.sim.peak_load, ctx.pcm_scenario, Dict{Int, Float64}()), y, NaN) for y in 1:n_years]
        margins = derated ./ peaks
        note!("capacity vs peak load",
            "nameplate/peak $(series(total ./ peaks; digits = 3)) | derated/peak $(series(margins; digits = 3))")
        soft!("derated capacity margin", all(m -> isfinite(m) && m >= ctx.min_capacity_margin, margins),
            "derated capacity / peak load minimum $(fmt(minimum(margins); digits = 3)) " *
            "(limit $(fmt(ctx.min_capacity_margin; digits = 3)))")
    end
end

# ---------------------------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------------------------

function main()
    results_dir = resolve_results_dir()
    isdir(results_dir) || error("Results directory not found: $(results_dir)")
    elapsed = arg_value("--elapsed-seconds", "")
    summary = Pair{String, String}["results_dir" => results_dir]
    isempty(elapsed) || push!(summary, "elapsed_hours" => string(round(parse(Float64, elapsed) / 3600; digits = 3)))

    years = checkpoint_years(results_dir)
    isempty(years) && error("No simulation_data_year_N.h5 checkpoints found in $(results_dir)")
    sim = quiet(() -> EAS.load_simulation(results_dir, last(years)))
    case = EAS.get_case(sim)
    ctx = (;
        results_dir, sim,
        simulation_years = EAS.get_simulation_years(case),
        total_horizon = EAS.get_total_horizon(case),
        pcm_scenario = EAS.get_pcm_scenario(case),
        da_resolution = EAS.get_da_resolution(case),
        rt_resolution = EAS.get_rt_resolution(case),
        scenarios = sort(collect(keys(EAS.get_resource_adequacy(sim)))),
        zones = String.(EAS.get_zones(sim)),
        price_cap = parse(Float64, arg_value("--price-cap", "10000")),
        peak_rtol = parse(Float64, arg_value("--peak-rtol", "0.02")),
        max_lost_load_share = parse(Float64, arg_value("--max-lost-load-share", "0.01")),
        min_capacity_margin = parse(Float64, arg_value("--min-capacity-margin", "1.0")),
    )
    println("Checking $(results_dir)\n  $(ctx.simulation_years) simulated years, scenarios $(ctx.scenarios), " *
            "DA/RT resolution $(ctx.da_resolution)/$(ctx.rt_resolution) min\n")
    hard!("run completed", last(years) == ctx.simulation_years,
        "last checkpoint year $(last(years)) of $(ctx.simulation_years)")

    guarded("result files written") do
        check_files(ctx)
    end
    peaks = check_load(ctx)
    check_market(ctx)
    check_resource_adequacy(ctx)
    check_capacity(ctx)

    for (y, peak) in enumerate(peaks)
        push!(summary, "md_peak_mw_year_$(y)" => fmt(peak))
    end
    n_fail = count(c -> c.status == "FAIL", CHECKS)
    n_warn = count(c -> c.status == "WARN", CHECKS)
    push!(summary, "hard_failures" => string(n_fail), "warnings" => string(n_warn),
        "status" => n_fail == 0 ? "passed" : "failed")
    CSV.write(joinpath(results_dir, "run_summary.csv"), DataFrame(key = first.(summary), value = last.(summary)))
    CSV.write(joinpath(results_dir, "run_checks.csv"),
        DataFrame(check = [c.name for c in CHECKS], level = [c.level for c in CHECKS],
            status = [c.status for c in CHECKS], detail = [c.detail for c in CHECKS]))
    println("\n$(n_fail) hard failure(s), $(n_warn) warning(s); wrote run_summary.csv and run_checks.csv")
    exit(n_fail == 0 ? 0 : 1)
end

main()
