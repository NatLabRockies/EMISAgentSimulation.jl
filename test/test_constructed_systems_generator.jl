using Test
using EMISAgentSimulation
using CSV
using DataFrames
using Dates
import PowerSystems

@testset "constructed system generator contract" begin
    base_dir = mktempdir()
    scenario = "scenario_1"
    sim_year = 1
    market_stage = "dayahead"
    horizon = 24
    interval = 1

    md_path = canonical_constructed_system_path(base_dir, scenario, sim_year, market_stage, horizon, interval; kind="md")
    sys_path = canonical_constructed_system_path(base_dir, scenario, sim_year, market_stage, horizon, interval; kind="sys")
    forecast_path = canonical_constructed_system_path(base_dir, scenario, sim_year, market_stage, horizon, interval; kind="forecast")

    create_sys_with_timeseries(
        nothing,
        base_dir,
        scenario,
        sim_year,
        market_stage,
        horizon,
        interval,
        sys_path;
        forecast_count = 3,
    )

    bundle = canonical_constructed_system_paths(base_dir, scenario, sim_year, market_stage, horizon, interval)
    @test endswith(bundle.sys, "DA_sys_EMIS_24hor_1int.json")
    @test endswith(bundle.md, "MD_sys_EMIS_24hor_1int.json")
    @test endswith(bundle.forecast, "MD_num_forecast_24hor_1int.txt")

    @test isfile(sys_path)
    @test isfile(forecast_path)
    @test occursin("constructed", read(sys_path, String))
    @test read(forecast_path, String) == "3\n"
end

@testset "canonical profile input contract" begin
    profile_file = joinpath(mktempdir(), "load.csv")
    open(profile_file, "w") do io
        println(io, "load_a,load_b")
        println(io, "1.0,2.0")
        println(io, "3.0,4.0")
    end

    @test EMISAgentSimulation._profile_values(profile_file, "load_a") == [1.0, 3.0]
    @test EMISAgentSimulation._profile_values(profile_file, :load_b) == [2.0, 4.0]
    @test EMISAgentSimulation._cyclic_profile([1.0, 2.0], 2, 5) == [2.0, 1.0, 2.0, 1.0, 2.0]
    @test_throws ErrorException EMISAgentSimulation._profile_values(profile_file, nothing)
    @test_throws ErrorException EMISAgentSimulation._profile_values(profile_file, "missing")

    @test endswith(canonical_timeseries_path("/tmp/input", "scenario_1", 2, "da", "load"),
        "scenario_1/sim_year_2/Load/DAY_AHEAD_regional_Load.csv")
    @test endswith(canonical_timeseries_path("/tmp/input", "scenario_1", 2, "rt", "wind"),
        "scenario_1/sim_year_2/WIND/REAL_TIME_wind.csv")
    @test endswith(canonical_timeseries_path("/tmp/input", "scenario_1", 2, "da", "pv"),
        "scenario_1/sim_year_2/PV/DAY_AHEAD_pv.csv")
    @test endswith(canonical_timeseries_path("/tmp/input", "scenario_1", 2, "da", "reserve"; product="SPIN"),
        "scenario_1/sim_year_2/Reserves/DAY_AHEAD_regional_SPIN.csv")

    defaults_file = joinpath(mktempdir(), "timeseries_defaults.csv")
    open(defaults_file, "w") do io
        println(io, "kind,market_stage,directory,filename")
        println(io, "load,dayahead,Demand,DA_load.csv")
    end
    @test endswith(canonical_timeseries_path(
        "/tmp/input", "scenario_1", 2, "da", "load"; defaults_file=defaults_file
    ), "scenario_1/sim_year_2/Demand/DA_load.csv")
end

@testset "canonical runtime load forecast normalization" begin
    PSY = PowerSystems
    mktempdir() do root
        cfg = SystemConfig(root, "load_test", ["zone_1"], ["scenario_1"], 0.1, 2021,
            Dict("area_1" => "zone_1", "zone_1" => "zone_1"),
            Dict{String, Dict{String, Any}}(), Dict{String, Dict{String, Any}}(),
            Dict{String, Vector{String}}(), 5, true)
        source = PSY.System(100.0)
        area = PSY.Area(; name = "area_1")
        PSY.add_component!(source, area)
        bus = PSY.ACBus(; number = 1, name = "bus", available = true,
            bustype = PSY.ACBusTypes.REF, angle = 0.0, magnitude = 1.0,
            voltage_limits = (min = 0.95, max = 1.05), base_voltage = 230.0, area = area)
        PSY.add_component!(source, bus)
        start = DateTime(2018, 1, 1)
        windows = Dict(start + Hour(24 * d) => ones(24) for d in 0:364)
        # Unequal maxima test zonal allocation; a zero load must remain zero.
        for (name, pmax) in (("small", 25.0), ("large", 75.0), ("zero", 0.0))
            load = PSY.PowerLoad(; name = name, available = true, bus = bus,
                active_power = 0.0, reactive_power = 0.0, base_power = 100.0,
                max_active_power = pmax, max_reactive_power = 0.0)
            PSY.add_component!(source, load)
            PSY.add_time_series!(source, load, PSY.Deterministic(;
                name = "max_active_power", data = windows, resolution = Hour(1),
                scaling_factor_multiplier = PSY.get_max_active_power))
        end
        # Reference peak is 200 MW; configured baseline is 100 MW (loadscaler = 2).
        profile = repeat(collect(range(100.0, 200.0; length = 24)), 365)
        reference = joinpath(root, "sim_year_1", "Load")
        for year in 1:2
            load_dir = joinpath(root, "sim_year_$(year)", "Load")
            mkpath(load_dir)
            growth = year == 1 ? 1.0 : 1.1
            table = DataFrame(Year = fill(2021, 8760), Month = ones(Int, 8760),
                Day = ones(Int, 8760), Period = repeat(1:24, 365), zone_1 = profile .* growth)
            for prefix in ("DAY_AHEAD", "REAL_TIME")
                CSV.write(joinpath(load_dir, "$(prefix)_regional_Load.csv"), table)
            end
        end
        for stage in ("dayahead", "realtime"), year in 1:2
            output = joinpath(root, "$(stage)_$(year).json")
            EMISAgentSimulation.create_sys_w_updated_ts(
                root, deepcopy(source), 2021, 2020 + year, stage, "baseline", 0.1,
                24, 24, output, true, nothing, nothing, output * ".txt";
                system_cfg = cfg,
                canonical_load_dir = joinpath(root, "sim_year_$(year)", "Load"),
                canonical_reference_load_dir = reference,
                canonical_wind_dir = root, canonical_pv_dir = root, canonical_reserves_dir = root)
            rebuilt = PSY.System(output; runchecks = false, time_series_directory = root)
            growth = year == 1 ? 1.0 : 1.1
            for units in (PSY.UnitSystem.NATURAL_UNITS, PSY.UnitSystem.SYSTEM_BASE)
                PSY.set_units_base_system!(rebuilt, units)
                to_mw = units == PSY.UnitSystem.NATURAL_UNITS ? 1.0 : PSY.get_base_power(rebuilt)
                hourly = zeros(8760)
                for load in PSY.get_components(PSY.PowerLoad, rebuilt)
                    actual = reduce(vcat, (PSY.get_time_series_values(PSY.Deterministic,
                        load, "max_active_power"; start_time = t, len = 24)
                        for t in PSY.get_forecast_initial_times(rebuilt))) .* to_mw
                    share = Dict("small" => 0.25, "large" => 0.75, "zero" => 0.0)[PSY.get_name(load)]
                    @test actual ≈ profile .* growth .* share ./ 2
                    hourly .+= actual
                end
                @test hourly ≈ profile .* growth ./ 2
                @test maximum(hourly) ≈ 100 * growth
            end
        end
    end
end
