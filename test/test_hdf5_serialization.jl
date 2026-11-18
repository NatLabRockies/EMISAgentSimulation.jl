# Test for HDF5 save/load roundtrip of AgentSimulation.
# Run from REPL after loading the package:
#   include("test/test_hdf5_serialization.jl")

using Test
using HDF5
using AxisArrays
using DataFrames
import PowerSystems as PSY

# ─────────────────────────────────────────────────────────────────────────────
# Recursive value comparator — returns true and is silent on success;
# prints a failure message and returns false on first mismatch.
# ─────────────────────────────────────────────────────────────────────────────

function vals_equal(a, b, path::String = "root")::Bool
    # AxisArrays are compared by content, not concrete type: HDF5 stores axis values
    # as plain vectors, so an `Axis{:year}(1:3)` range comes back as `Axis{:year}([1, 2, 3])`.
    # That changes the type parameters but not the axis names, values, or data.
    if a isa AxisArrays.AxisArray && b isa AxisArrays.AxisArray
        if eltype(a) != eltype(b) || ndims(a) != ndims(b)
            println("FAIL [$path]: AxisArray $(eltype(a))/$(ndims(a))D ≠ $(eltype(b))/$(ndims(b))D")
            return false
        end
        if AxisArrays.axisnames(a) != AxisArrays.axisnames(b)
            println("FAIL [$path]: AxisArray axis names $(AxisArrays.axisnames(a)) ≠ $(AxisArrays.axisnames(b))")
            return false
        end
        if collect(a) != collect(b)
            @info "FAIL [$path]: AxisArray data differs"
            return false
        end
        vals_a, vals_b = AxisArrays.axisvalues(a), AxisArrays.axisvalues(b)
        for i in eachindex(vals_a)
            if collect(vals_a[i]) != collect(vals_b[i])
                println("FAIL [$path]: AxisArray axis $i values differ")
                return false
            end
        end
        return true
    end
    if typeof(a) != typeof(b)
        println("FAIL [$path]: type $(typeof(a)) ≠ $(typeof(b))")
        return false
    end
    if a isa Dict
        if keys(a) != keys(b)
            @info "FAIL [$path]: Dict keys $(keys(a)) ≠ $(keys(b))"
            return false
        end
        return all(vals_equal(a[k], b[k], "$path.$k") for k in keys(a))
    end
    if a isa AbstractArray
        if size(a) != size(b)
            @info "FAIL [$path]: array size $(size(a)) ≠ $(size(b))"
            return false
        end
        return a == b
    end
    if a isa NamedTuple
        for k in keys(a)
            vals_equal(a[k], b[k], "$path.$k") || return false
        end
        return true
    end
    # PSY operation costs (Thermal/Renewable/HydroGenerationCost) don't define `==`,
    # so two costs with identical contents compare unequal. Compare their fields;
    # the cost curves inside (CostCurve / FuelCurve) do support `==`.
    if a isa PSY.OperationalCost
        return all(vals_equal(getfield(a, f), getfield(b, f), "$path.$f")
                   for f in fieldnames(typeof(a)))
    end
    ok = (a == b)
    ok || @info "FAIL [$path]: $a ≠ $b"
    return ok
end

# ─────────────────────────────────────────────────────────────────────────────
# Builders
# ─────────────────────────────────────────────────────────────────────────────

SCENS = ["S1", "S2"]
N_YRS = 3
N_HRS = 4   # hours per year in tiny test arrays
PARAMS = [:cap_factor, :fuel_price]

function make_axis_2d()
    AxisArrays.AxisArray(
        rand(N_YRS, N_HRS),
        AxisArrays.Axis{:year}(1:N_YRS),
        AxisArrays.Axis{:hour}(1:N_HRS),
    )
end

function make_axis_1d()
    AxisArrays.AxisArray(rand(N_YRS), AxisArrays.Axis{:year}(1:N_YRS))
end

function make_finance()
    realized_profit = AxisArrays.AxisArray(
        rand(N_YRS, 2),
        AxisArrays.Axis{:year}(1:N_YRS),
        AxisArrays.Axis{:product}([:energy, :capacity]),
    )
    scenario_profit = Dict(s => [make_axis_2d() for _ in 1:N_YRS] for s in SCENS)
    scenario_total_utilization = Dict(s => rand(N_YRS, N_HRS) for s in SCENS)
    scenario_npv = Dict(s => rand(N_YRS) for s in SCENS)
    scenario_utility = Dict(s => rand(N_YRS) for s in SCENS)

    return Finance(
        rand(N_YRS),         # investment_cost
        1.5e6,               # effective_investment_cost
        fill(1.0, N_YRS),   # preference_multiplier
        2,                   # lag_time
        25,                  # life_time
        20,                  # capex_years
        50_000.0,            # fixed_OM_cost
        rand(N_YRS),         # queue_cost
        scenario_total_utilization,
        scenario_profit,
        realized_profit,
        0.08,                # discount_rate
        scenario_npv,
        rand(N_YRS),         # expected_npv
        scenario_utility,
        rand(N_YRS),         # expected_utility
        rand(N_YRS),         # annual_cashflow
        "TestInvestor",      # ownedby
    )
end

function make_products()
    return Product[
        Energy(
            :energy,
            Dict(s => rand(N_YRS, N_HRS) for s in SCENS),
            3.5,   # marginal_cost
            100.0, # expected_production
        ),
        Capacity(
            :capacity,
            # derating and accepted_perc are nested as: scenario → season → value.
            # Two seasons exercise the nested structure; the project round-trip test
            # below then validates save_product!/load_product for the new layout.
            Dict(s => Dict("summer" => 0.82, "winter" => 0.55) for s in SCENS),  # derating
            Dict(s => Dict("summer" => rand(N_YRS), "winter" => rand(N_YRS)) for s in SCENS),  # accepted_perc
            200.0, # capacity_bid
        ),
        OperatingReserve{ReserveUpEMIS}(:reg_up, 0.1, 1.0),
        OperatingReserve{ReserveDownEMIS}(:reg_dn, 0.1, 1.0),
        REC(:rec, 50.0, rand(N_YRS), 15.0),
        Inertia(:inertia, true, 5.0, 0.5),
        CarbonTax(:co2, 0.05, 8.0, 3.5, rand(N_YRS)),
    ]
end

function make_thermal_tech()
    return ThermalTech(
        "CCGT",
        "Gas",
        (min = 50.0, max = 400.0),
        (up = 100.0, down = 100.0),
        (up = 2.0, down = 4.0),
        PSY.ThermalGenerationCost(;
            # CostCurve takes a ValueCurve; piecewise (x, y) points are an
            # InputOutputCurve over PiecewiseLinearData (a PiecewisePointCurve).
            variable = PSY.CostCurve(
                PSY.InputOutputCurve(PSY.PiecewiseLinearData([(0.0, 0.0), (400.0, 1200.0)])),
            ),
            fixed = 5000.0,
            start_up = (hot = 500.0, warm = 1000.0, cold = 2000.0),
            shut_down = 200.0,
        ),
        4.5,   # fuel_cost
        [(0.0, 8.0), (400.0, 9.5)],  # heat_rate_curve
        "BusA",
        "ZoneA",
        0.05,  # FOR
        24,    # MTTR
    )
end

function make_renewable_tech()
    return RenewableTech(
        "WT",
        (min = 0.0, max = 100.0),
        nothing,
        PSY.RenewableGenerationCost(;
            # Linear variable cost and a piecewise-point curtailment cost: two
            # different value-curve types through the same save/load path.
            variable = PSY.CostCurve(PSY.LinearCurve(0.0)),
            curtailment_cost = PSY.CostCurve(
                PSY.InputOutputCurve(PSY.PiecewiseLinearData([(0.0, 0.0), (100.0, 5.0)])),
            ),
            fixed = 0.0,
        ),
        "BusB",
        "ZoneB",
        0.01,
        8,
    )
end

function make_hydro_tech()
    return HydroTech(
        "Hydro",
        (min = 0.0, max = 200.0),
        (up = 50.0, down = 50.0),
        nothing,
        PSY.HydroGenerationCost(;
            # Piecewise incremental curve (PiecewiseStepData), the most common
            # non-linear form in the RTS-based systems, in device-base units.
            variable = PSY.CostCurve(;
                value_curve = PSY.PiecewiseIncrementalCurve(10.0, [20.0, 100.0, 200.0], [15.0, 22.0]),
                power_units = PSY.UnitSystem.DEVICE_BASE,
            ),
            fixed = 1000.0,
        ),
        "BusC",
        "ZoneA",
        0.02,
        12,
    )
end

function make_battery_tech()
    return BatteryTech(
        "Li-ion",
        (min = 0.0, max = 50.0),
        (min = 0.0, max = 50.0),
        nothing,
        (min = 0.0, max = 200.0),
        (min = 0.2, max = 0.9),
        0.5,
        200.0,
        0.5,
        (in = 0.92, out = 0.92),
        "BusD",
        "ZoneB",
        0.01,
        4,
        50.0,
    )
end

function make_scenario()
    pv = [make_axis_2d() for _ in 1:N_YRS]
    return Scenario("S1", 0.6, Dict("cap_factor" => 1.05, "fuel_price" => 0.95), pv)
end

function make_scenario_no_multipliers()
    pv = [make_axis_2d() for _ in 1:N_YRS]
    return Scenario("S2", 0.4, nothing, pv)
end

function make_market_prices()
    ep = Dict(
        s => AxisArrays.AxisArray(
            rand(N_YRS, N_HRS, 2),
            AxisArrays.Axis{:year}(1:N_YRS),
            AxisArrays.Axis{:hour}(1:N_HRS),
            AxisArrays.Axis{:zone}(["ZoneA", "ZoneB"]),
        ) for s in SCENS
    )
    rp = Dict("reg_up" => Dict(s => rand(N_YRS, N_HRS) for s in SCENS))
    # Capacity prices are scenario -> season -> AxisArray (MarketPrices.capacity_price).
    cp = Dict(s => Dict("summer" => make_axis_1d(), "winter" => make_axis_1d()) for s in SCENS)
    rcp = Dict(s => make_axis_1d() for s in SCENS)
    inp = Dict(s => make_axis_2d() for s in SCENS)
    return MarketPrices(ep, rp, cp, rcp, inp)
end

function make_investor(name::String)
    projects = Project{<:BuildPhase}[
        ThermalGenEMIS{Existing}(
            "CC_plant",
            make_thermal_tech(),
            1,
            1,
            30,
            30,
            make_products(),
            make_finance(),
        ),
        RenewableGenEMIS{Option}(
            "wind_opt",
            make_renewable_tech(),
            3,
            5,
            99,
            99,
            make_products(),
            make_finance(),
        ),
        HydroGenEMIS{Planned}(
            "hydro_pl",
            make_hydro_tech(),
            2,
            4,
            50,
            50,
            make_products(),
            make_finance(),
        ),
        BatteryEMIS{Queue}(
            "batt_q",
            make_battery_tech(),
            2,
            3,
            25,
            25,
            make_products(),
            make_finance(),
        ),
    ]
    return Investor(
        name,
        "/data/$name",
        projects,
        [:energy, :capacity, :rec],
        rand(N_YRS),                   # carbon_tax
        make_market_prices(),
        24,                             # rep_period_interval
        Dict(s => Dict(y => rand(N_HRS) for y in 1:N_YRS) for s in SCENS),  # rep_hour_weight
        4,                              # avg_block_size
        true,                           # fixed_block_size
        Dict(s => Dict(y => rand(Int, N_HRS, N_HRS) for y in 1:N_YRS) for s in SCENS),  # chron_weights
        Perfect([make_scenario(), make_scenario_no_multipliers()]),
        1.0,                            # cap_cost_multiplier
        (min = 0.8, max = 1.2),             # preference_multiplier_range
        Dict(("wind_opt", "CC_plant") => rand(N_YRS)),  # portfolio_preference_multipliers
        5,                              # max_annual_projects
        RiskNeutral(),
        3,                              # retirement_lookback
    )
end

function make_simulation()
    zones = ["ZoneA", "ZoneB"]
    lines = [ZonalLine("A_to_B", "ZoneA", "ZoneB", 500.0)]
    rep_periods =
        Dict(s => Dict(y => Dict(h => h for h in 1:N_HRS) for y in 1:N_YRS) for s in SCENS)
    hour_weight = Dict(s => Dict(y => rand(N_HRS) for y in 1:N_YRS) for s in SCENS)
    peak_load = Dict(s => Dict(y => 1000.0 * y for y in 1:N_YRS) for s in SCENS)
    markets = Dict(:energy => true, :capacity => true, :rec => false)
    derating_df = DataFrame(; technology = ["CCGT", "WT"], derate = [0.9, 1.0])
    derating_data = Dict("thermal" => derating_df)
    ra = ResourceAdequacy(
        Dict("ZoneA" => 0.1, "ZoneB" => 0.1),
        rand(N_YRS),
        [Dict("eue" => rand(), "lolp" => rand()) for _ in 1:N_YRS],
    )

    # CaseDefinition has no zero-argument constructor; use the keyword constructor.
    # Its constructor calls make_case_data_dir, which copies
    # {base_dir}/Homogeneous into {base_dir}/{name} when that case directory is
    # missing or empty. So use a fresh temp base_dir (removed when Julia exits)
    # with an empty Homogeneous/ source: the copy then only creates an empty case
    # directory. The other paths are only stored. The solver is not serialized to
    # HDF5, so any optimizer works here.
    case_root = mktempdir()
    mkpath(joinpath(case_root, "Homogeneous"))
    case = CaseDefinition(
        "hdf5_roundtrip_test",
        case_root,
        joinpath(case_root, "sys"),
        joinpath(case_root, "scratch"),
        joinpath(case_root, "outages"),
        joinpath(case_root, "timeseries"),
        EMISAgentSimulation.JuMP.optimizer_with_attributes(EMISAgentSimulation.HiGHS.Optimizer),
    )

    return AgentSimulation(
        case,
        "/results/test",
        2,
        nothing, nothing, nothing,
        Dict{String, PSY.System}(),
        zones,
        lines,
        rep_periods,
        24,
        hour_weight,
        peak_load,
        markets,
        rand(N_YRS),    # carbon_tax
        rand(N_YRS),    # rec_requirement
        [make_investor("inv1"), make_investor("inv2")],
        derating_data,
        Dict("S1" => ra),
    )
end

# ─────────────────────────────────────────────────────────────────────────────
# Field-by-field comparison helpers
# ─────────────────────────────────────────────────────────────────────────────

function compare_products(orig::Vector, loaded::Vector, path::String)
    @test length(orig) == length(loaded)
    # Products are compared by position, so order must survive the round trip
    # (the fixture's product order is not alphabetical).
    @test [p.name for p in loaded] == [p.name for p in orig]
    for (i, (po, pl)) in enumerate(zip(orig, loaded))
        @test typeof(po) == typeof(pl)
        for f in fieldnames(typeof(po))
            @test vals_equal(getfield(po, f), getfield(pl, f), "$path.product[$i].$f")
        end
    end
end

function compare_tech(orig, loaded, path::String)
    @test typeof(orig) == typeof(loaded)
    for f in fieldnames(typeof(orig))
        @test vals_equal(getfield(orig, f), getfield(loaded, f), "$path.tech.$f")
    end
end

function compare_finance(orig::Finance, loaded::Finance, path::String)
    for f in fieldnames(Finance)
        @test vals_equal(getfield(orig, f), getfield(loaded, f), "$path.finance.$f")
    end
end

function compare_project(orig, loaded, path::String)
    @test typeof(orig) == typeof(loaded)
    compare_tech(orig.tech, loaded.tech, "$path.$(orig.name)")
    compare_finance(orig.finance_data, loaded.finance_data, "$path.$(orig.name)")
    compare_products(orig.products, loaded.products, "$path.$(orig.name)")
    for f in (:name, :decision_year, :construction_year, :retirement_year, :end_life_year)
        @test vals_equal(getfield(orig, f), getfield(loaded, f), "$path.$(orig.name).$f")
    end
end

function compare_market_prices(orig::MarketPrices, loaded::MarketPrices, path::String)
    for f in fieldnames(MarketPrices)
        @test vals_equal(getfield(orig, f), getfield(loaded, f), "$path.market_prices.$f")
    end
end

function compare_forecast(orig, loaded, path::String)
    @test typeof(orig) == typeof(loaded)
    @test length(orig.scenario_data) == length(loaded.scenario_data)
    for (i, (so, sl)) in enumerate(zip(orig.scenario_data, loaded.scenario_data))
        @test so.name == sl.name
        @test so.probability ≈ sl.probability
        @test vals_equal(
            so.parameter_multipliers,
            sl.parameter_multipliers,
            "$path.scenario[$i].multipliers",
        )
        @test length(so.parameter_values) == length(sl.parameter_values)
        for (j, (pvo, pvl)) in enumerate(zip(so.parameter_values, sl.parameter_values))
            @test vals_equal(pvo, pvl, "$path.scenario[$i].parameter_values[$j]")
        end
    end
end

function compare_investor(orig::Investor, loaded::Investor, path::String)
    for f in (:name, :data_dir, :avg_block_size, :fixed_block_size,
        :rep_period_interval, :cap_cost_multiplier, :max_annual_projects,
        :retirement_lookback, :carbon_tax, :markets)
        @test vals_equal(getfield(orig, f), getfield(loaded, f), "$path.$f")
    end
    @test vals_equal(
        orig.preference_multiplier_range,
        loaded.preference_multiplier_range,
        "$path.preference_multiplier_range",
    )
    @test typeof(orig.risk_preference) == typeof(loaded.risk_preference)
    compare_market_prices(orig.market_prices, loaded.market_prices, path)
    compare_forecast(orig.forecast, loaded.forecast, path)
    @test vals_equal(orig.rep_hour_weight, loaded.rep_hour_weight, "$path.rep_hour_weight")
    @test vals_equal(orig.chron_weights, loaded.chron_weights, "$path.chron_weights")
    @test vals_equal(
        orig.portfolio_preference_multipliers,
        loaded.portfolio_preference_multipliers,
        "$path.portfolio_pref_mult",
    )

    @test length(orig.projects) == length(loaded.projects)
    # Projects must come back in their original order, not sorted: the fixture's
    # order (CC_plant, wind_opt, hydro_pl, batt_q) is deliberately not alphabetical.
    @test [p.name for p in loaded.projects] == [p.name for p in orig.projects]
    for (po, pl) in zip(orig.projects, loaded.projects)
        compare_project(po, pl, path)
    end
end

function compare_simulation(orig::AgentSimulation, loaded::AgentSimulation)
    @testset "Top-level scalars" begin
        @test orig.results_dir == loaded.results_dir
        @test orig.iteration_year == loaded.iteration_year
        @test orig.rep_period_interval == loaded.rep_period_interval
        @test orig.carbon_tax == loaded.carbon_tax
        @test orig.rec_requirement == loaded.rec_requirement
        @test orig.zones == loaded.zones
        @test orig.markets == loaded.markets
    end

    @testset "Lines" begin
        @test length(orig.lines) == length(loaded.lines)
        for (lo, ll) in zip(orig.lines, loaded.lines)
            for f in fieldnames(ZonalLine)
                @test vals_equal(getfield(lo, f), getfield(ll, f), "line.$(lo.name).$f")
            end
        end
    end

    @testset "rep_periods / hour_weight / peak_load" begin
        @test vals_equal(orig.rep_periods, loaded.rep_periods, "rep_periods")
        @test vals_equal(orig.hour_weight, loaded.hour_weight, "hour_weight")
        @test vals_equal(orig.peak_load, loaded.peak_load, "peak_load")
    end

    @testset "derating_data" begin
        @test keys(orig.derating_data) == keys(loaded.derating_data)
        for k in keys(orig.derating_data)
            @test orig.derating_data[k] == loaded.derating_data[k]
        end
    end

    @testset "resource_adequacy" begin
        @test keys(orig.resource_adequacy) == keys(loaded.resource_adequacy)
        for k in keys(orig.resource_adequacy)
            rao, ral = orig.resource_adequacy[k], loaded.resource_adequacy[k]
            @test vals_equal(rao.targets, ral.targets, "ra.$k.targets")
            @test vals_equal(rao.delta_irm, ral.delta_irm, "ra.$k.delta_irm")
            @test vals_equal(rao.metrics, ral.metrics, "ra.$k.metrics")
        end
    end

    @testset "Investors" begin
        @test length(orig.investors) == length(loaded.investors)
        orig_sorted = sort(orig.investors, by = i -> i.name)
        loaded_sorted = sort(loaded.investors, by = i -> i.name)
        for (io, il) in zip(orig_sorted, loaded_sorted)
            @testset "Investor $(io.name)" begin
                compare_investor(io, il, "investor.$(io.name)")
            end
        end
    end
end

# ─────────────────────────────────────────────────────────────────────────────
# Run
# ─────────────────────────────────────────────────────────────────────────────

# Re-enabled (was commented out on master on 2026-10-14 because make_simulation()
# called a nonexistent CaseDefinition()): make_simulation() now builds a real
# CaseDefinition in a temp directory, and the test uses the HDF5-level save/load API.
@testset "HDF5 AgentSimulation roundtrip" begin
    tmpfile = tempname() * ".h5"
    try
        sim_orig = make_simulation()

        # Use the HDF5-level API (save_simulation! / load_simulation(f)). The
        # directory-level save_simulation(sim, dir, year) also writes PSY systems,
        # which this fixture doesn't have.
        @testset "save_simulation!" begin
            h5open(tmpfile, "w") do f
                EMISAgentSimulation.save_simulation!(f, sim_orig)
            end
            @test isfile(tmpfile)
        end

        sim_loaded = nothing
        @testset "load_simulation" begin
            sim_loaded = h5open(tmpfile, "r") do f
                EMISAgentSimulation.load_simulation(f)
            end
            @test sim_loaded isa AgentSimulation
        end

        @testset "roundtrip comparison" begin
            compare_simulation(sim_orig, sim_loaded)
        end
    finally
        isfile(tmpfile) && rm(tmpfile)
    end
end

# ─────────────────────────────────────────────────────────────────────────────
# Backward compatibility — legacy (pre-seasonal) .h5 files load as annual mode
# ─────────────────────────────────────────────────────────────────────────────
#
# Old saved files predate the seasonal capacity-markets feature: they stored
# capacity data in the FLAT layout. These tests write the legacy layouts by hand
# and assert the current loaders map them into the "annual" season shape, while
# new seasonal layouts still load — so old result files remain openable for
# post-processing.

@testset "Backward compatibility — legacy flat .h5 loads as annual" begin
    @testset "load_capacity_price_seasonal: legacy flat AxisArray -> annual" begin
        tmpfile = tempname() * ".h5"
        try
            legacy = AxisArrays.AxisArray(reshape([42.0], 1), [1])
            h5open(tmpfile, "w") do f
                EMISAgentSimulation.save_axisarray!(HDF5.create_group(f, "capacity_price"), legacy)
            end
            loaded = h5open(tmpfile, "r") do f
                EMISAgentSimulation.load_capacity_price_seasonal(f["capacity_price"])
            end
            @test collect(keys(loaded)) == ["annual"]
            @test loaded["annual"][1] == 42.0
        finally
            isfile(tmpfile) && rm(tmpfile)
        end
    end

    @testset "load_capacity_price_seasonal: new seasonal layout still loads" begin
        tmpfile = tempname() * ".h5"
        try
            price = Dict("summer" => AxisArrays.AxisArray(reshape([1.0], 1), [1]),
                        "winter" => AxisArrays.AxisArray(reshape([2.0], 1), [1]))
            h5open(tmpfile, "w") do f
                EMISAgentSimulation.save_capacity_price_seasonal!(
                    HDF5.create_group(f, "capacity_price"), price)
            end
            loaded = h5open(tmpfile, "r") do f
                EMISAgentSimulation.load_capacity_price_seasonal(f["capacity_price"])
            end
            @test Set(keys(loaded)) == Set(["summer", "winter"])
        finally
            isfile(tmpfile) && rm(tmpfile)
        end
    end

    @testset "load_capacity_bids_seasonal: legacy flat Dict{name=>frac} -> annual" begin
        tmpfile = tempname() * ".h5"
        try
            h5open(tmpfile, "w") do f
                EMISAgentSimulation._save_dict_str_float!(
                    HDF5.create_group(f, "capacity_accepted_bids"),
                    Dict("Gen1" => 1.0, "Gen2" => 0.5))
            end
            loaded = h5open(tmpfile, "r") do f
                EMISAgentSimulation.load_capacity_bids_seasonal(f["capacity_accepted_bids"])
            end
            @test collect(keys(loaded)) == ["annual"]
            @test loaded["annual"]["Gen1"] == 1.0
            @test loaded["annual"]["Gen2"] == 0.5
        finally
            isfile(tmpfile) && rm(tmpfile)
        end
    end

    @testset "load_product(Capacity): legacy keys/values derating -> annual" begin
        tmpfile = tempname() * ".h5"
        try
            h5open(tmpfile, "w") do f
                g = HDF5.create_group(f, "product")
                HDF5.attributes(g)["product_type"] = "Capacity"
                write(g, "name", "Capacity")
                write(g, "capacity_bid", 100.0)
                # The layout real pre-seasonal checkpoints use: parallel datasets, in
                # Dict iteration order rather than sorted, so pairing is by position.
                der = HDF5.create_group(g, "derating")
                write(der, "keys", ["scenario_2", "scenario_1"])
                write(der, "values", [0.85, 0.71])
                ap = HDF5.create_group(g, "accepted_perc")
                write(ap, "scenario_1", [0.7, 0.8])
            end
            prod = h5open(tmpfile, "r") do f
                EMISAgentSimulation.load_product(f["product"])
            end
            @test prod isa Capacity
            @test Set(keys(prod.derating)) == Set(["scenario_1", "scenario_2"])
            @test get_derating(prod, "scenario_2", "annual") == 0.85
            @test get_derating(prod, "scenario_1", "annual") == 0.71
            @test get_accepted_perc(prod, "scenario_1", "annual") == [0.7, 0.8]
        finally
            isfile(tmpfile) && rm(tmpfile)
        end
    end

    @testset "load_case_definition: missing seasonal_capacity_market defaults to false" begin
        # Pre-seasonal checkpoints have no such field. Start from a current case and
        # remove it, so this keeps working as CaseDefinition gains other fields.
        case_root = mktempdir()
        mkpath(joinpath(case_root, "Homogeneous"))
        case = CaseDefinition(
            "legacy_case", case_root, case_root, case_root, case_root, case_root,
            EMISAgentSimulation.JuMP.optimizer_with_attributes(EMISAgentSimulation.HiGHS.Optimizer);
            seasonal_capacity_market = true,
        )
        tmpfile = tempname() * ".h5"
        try
            h5open(tmpfile, "w") do f
                g = HDF5.create_group(f, "case")
                EMISAgentSimulation.save_case_definition!(g, case)
                # A current file keeps its saved value...
                @test EMISAgentSimulation.get_seasonal_capacity_market(
                    EMISAgentSimulation.load_case_definition(g)) == true
                # ...and an old file without the field loads in annual mode.
                HDF5.delete_object(g, "seasonal_capacity_market")
                @test EMISAgentSimulation.get_seasonal_capacity_market(
                    EMISAgentSimulation.load_case_definition(g)) == false
            end
        finally
            isfile(tmpfile) && rm(tmpfile)
        end
    end

    @testset "load_market_prices capacity_price: legacy flat scenario->AxisArray -> annual" begin
        tmpfile = tempname() * ".h5"
        try
            legacy_price = AxisArrays.AxisArray(reshape([15.0], 1), [1])
            h5open(tmpfile, "w") do f
                cp = HDF5.create_group(f, "capacity_price")
                HDF5.attributes(cp)["is_nothing"] = false
                EMISAgentSimulation.save_axisarray!(HDF5.create_group(cp, "scenario_1"), legacy_price)
            end
            loaded = h5open(tmpfile, "r") do f
                EMISAgentSimulation._load_optional_capacity_price(f["capacity_price"])
            end
            @test collect(keys(loaded)) == ["scenario_1"]
            @test collect(keys(loaded["scenario_1"])) == ["annual"]
            @test loaded["scenario_1"]["annual"][1] == 15.0
        finally
            isfile(tmpfile) && rm(tmpfile)
        end
    end

    @testset "MarketPrices capacity_price: new 2-level layout round-trips" begin
        tmpfile = tempname() * ".h5"
        try
            price = Dict("scenario_1" => Dict(
                "summer" => AxisArrays.AxisArray(reshape([10.0], 1), [1]),
                "winter" => AxisArrays.AxisArray(reshape([20.0], 1), [1])))
            h5open(tmpfile, "w") do f
                EMISAgentSimulation._save_optional_capacity_price!(
                    HDF5.create_group(f, "capacity_price"), price)
            end
            loaded = h5open(tmpfile, "r") do f
                EMISAgentSimulation._load_optional_capacity_price(f["capacity_price"])
            end
            @test Set(keys(loaded["scenario_1"])) == Set(["summer", "winter"])
            @test loaded["scenario_1"]["winter"][1] == 20.0
        finally
            isfile(tmpfile) && rm(tmpfile)
        end
    end

    @testset "MarketPrices capacity_price: nothing round-trips" begin
        tmpfile = tempname() * ".h5"
        try
            h5open(tmpfile, "w") do f
                EMISAgentSimulation._save_optional_capacity_price!(
                    HDF5.create_group(f, "capacity_price"), nothing)
            end
            loaded = h5open(tmpfile, "r") do f
                EMISAgentSimulation._load_optional_capacity_price(f["capacity_price"])
            end
            @test isnothing(loaded)
        finally
            isfile(tmpfile) && rm(tmpfile)
        end
    end
end

# ─────────────────────────────────────────────────────────────────────────────
# Plan 5d — whole-file round-trip of seasonal capacity data
#
# The tests above check the capacity helpers in isolation. These go through the
# public file writers/readers used in a run:
#   save_expected_market_data / load_expected_market_data   (CEM output, per investor)
#   save_realized_market_data / load_realized_market_data   (actual clearing, per year)
# They check that a two-season capacity_price (and, for realized data,
# capacity_accepted_bids) survives with the right season keys and values, and that a
# legacy file with the flat pre-seasonal layout loads as "annual".
#
# Non-capacity fields get small placeholder values of the right shape; they are only
# there because the writers take every field positionally.
# ─────────────────────────────────────────────────────────────────────────────

_ax1() = AxisArrays.AxisArray([1.0, 2.0], 1:2)
_ax2() = AxisArrays.AxisArray([1.0 2.0; 3.0 4.0], ["a", "b"], 1:2)
_dict_mat() = Dict("Gen1" => [1.0 2.0; 3.0 4.0])
_dict_float() = Dict("Gen1" => 1.5)

# Positional arguments of save_expected_market_data after `path`, in signature order.
function _expected_market_args(capacity_price)
    return (
        capacity_price,                                  # capacity_price
        _ax2(),                                          # energy_price
        _dict_mat(),                                     # reserve_price
        _ax1(),                                          # rec_price
        _ax1(),                                          # inertia_price
        _dict_mat(),                                     # capacity_factors
        _dict_mat(),                                     # total_utilization
        Dict("Gen1" => [0.25, 1.0]),                     # capacity_accepted_perc
        Dict("option_WT_Z1" => 2),                       # new_options
        _dict_float(),                                   # new_options_by_type
        _ax1(), _ax1(), _ax1(),                          # REC_slack, REC_supply, REC_demand
        ["Gen1"],                                        # rps_compliant_projects
        _ax1(),                                          # rec_requirement
        _ax2(), _ax2(),                                  # investment, retirement
        _dict_float(), _dict_float(),                    # max_new_options, max_gen
        _ax2(),                                          # demand_e
        Dict("scenario_1" => Dict(1 => [1.0, 2.0])),     # rep_hour_weight
        _ax2(), _ax2(), _ax2(), _ax2(), _ax2(),          # p_e_print, in/out_flow_print, v_e_print, p_in_print
        _dict_float(), _dict_float(),                    # linepowerlimit, rec_correction
        _ax2(),                                          # p_e_storage_print
        Dict("Z1" => ["Batt1"], "Z2" => String[]),       # zone_storage
        Dict("Z1" => ["Gen1"]),                          # zone_projects
        Dict("Z1" => ["L1"]), Dict("Z1" => String[]),    # lines_to_zone, lines_from_zone
        _dict_float(),                                   # init_storage
        _ax2(),                                          # p_e_detail
        _dict_float(),                                   # remaining_buildtime
        ["Gen1", "Batt1"], ["Gen1"], ["Batt1"],          # projects, generator_projects, storage_projects
        ["Reg_Up"], ["ORDC"], ["Reg_Down"],              # reserve_up/ordc/reserve_down products
        _ax2(), _ax2(), _ax2(), _ax2(),                  # p_in_{ru,ordc,inertia,rd}_detail
        _ax2(), _ax2(), _ax2(), _ax2(),                  # p_out_{ru,ordc,inertia,rd}_detail
    )
end

# Positional arguments of save_realized_market_data after `path`, in signature order.
function _realized_market_args(capacity_price, capacity_accepted_bids)
    reserve_perc = Dict("Reg_Up" => Dict("Gen1" => [0.1 0.2]))
    return (
        capacity_price,                                  # capacity_price
        _ax2(), _ax2(), _ax2(),                          # energy_price_ed/uc/md
        _dict_mat(), _dict_mat(), _dict_mat(),           # reserve_price_ed/uc/md
        _ax1(), _ax1(),                                  # rec_price, inertia_price
        _dict_mat(), _dict_mat(), _dict_mat(),           # capacity_factors_md/uc/ed
        reserve_perc, reserve_perc, reserve_perc,        # reserve_perc_md/uc/ed
        capacity_accepted_bids,                          # capacity_accepted_bids
        _dict_float(),                                   # rec_accepted_bids
        _dict_mat(), _dict_mat(), _dict_mat(),           # inertia_perc, start_up_costs, shut_down_costs
        _ax2(), _ax2(), _ax2(),                          # energy_voll, energy_voll_uc, energy_voll_md
        _dict_mat(), _dict_mat(), _dict_mat(),           # reserve_voll, reserve_voll_uc, reserve_voll_md
        _ax1(),                                          # inertia_voll
        Vector{Union{String, Float64}}[["Gen1", 5.0, 100.0]],  # rec_supply_curve
        1000.0,                                          # rec_energy_requirment
        0.9,                                             # cet_achieved_ratio
    )
end

# Replace a group in an existing file with the legacy flat layout, as files written
# before the seasonal change have it.
function _rewrite_as_legacy!(path, group_name, write_flat!)
    h5open(path, "r+") do f
        HDF5.delete_object(f, group_name)
        write_flat!(HDF5.create_group(f, group_name))
    end
end

@testset "Plan 5d — expected/realized market data files: seasonal capacity round-trip" begin
    summer = AxisArrays.AxisArray([10.0, 11.0, 12.0], 1:3)
    winter = AxisArrays.AxisArray([20.0, 21.0, 22.0], 1:3)
    seasonal_price = Dict("summer" => summer, "winter" => winter)

    @testset "expected market data: two seasons survive with keys and values" begin
        tmpfile = tempname() * ".h5"
        try
            EMISAgentSimulation.save_expected_market_data(tmpfile, _expected_market_args(seasonal_price)...)
            loaded = EMISAgentSimulation.load_expected_market_data(tmpfile)

            cap = loaded["capacity_price"]
            @test Set(keys(cap)) == Set(["summer", "winter"])
            for (season, orig) in seasonal_price
                @test collect(cap[season]) == collect(orig)
                @test AxisArrays.axisvalues(cap[season]) == AxisArrays.axisvalues(orig)
            end
            # investor_iteration.jl takes the seasons from these keys and passes the
            # nested dict to set_capacity_price! (Dict{String, <:AxisArray}).
            @test cap isa Dict{String, <:AxisArrays.AxisArray{Float64, 1}}
            # The rest of the file is unaffected by the capacity layout.
            @test loaded["capacity_accepted_perc"] == Dict("Gen1" => [0.25, 1.0])
        finally
            isfile(tmpfile) && rm(tmpfile)
        end
    end

    @testset "expected market data: annual mode writes and reads a single \"annual\" season" begin
        tmpfile = tempname() * ".h5"
        try
            annual = Dict("annual" => AxisArrays.AxisArray([30.0, 31.0], 1:2))
            EMISAgentSimulation.save_expected_market_data(tmpfile, _expected_market_args(annual)...)
            cap = EMISAgentSimulation.load_expected_market_data(tmpfile)["capacity_price"]
            @test collect(keys(cap)) == ["annual"]
            @test collect(cap["annual"]) == [30.0, 31.0]
        finally
            isfile(tmpfile) && rm(tmpfile)
        end
    end

    @testset "expected market data: legacy flat capacity_price loads as \"annual\"" begin
        tmpfile = tempname() * ".h5"
        try
            EMISAgentSimulation.save_expected_market_data(tmpfile, _expected_market_args(seasonal_price)...)
            legacy = AxisArrays.AxisArray([40.0, 41.0], 1:2)
            _rewrite_as_legacy!(tmpfile, "capacity_price",
                g -> EMISAgentSimulation.save_axisarray!(g, legacy))

            cap = EMISAgentSimulation.load_expected_market_data(tmpfile)["capacity_price"]
            @test collect(keys(cap)) == ["annual"]
            @test collect(cap["annual"]) == [40.0, 41.0]
        finally
            isfile(tmpfile) && rm(tmpfile)
        end
    end

    realized_price = Dict("summer" => AxisArrays.AxisArray(reshape([15000.0], 1), [1]),
                          "winter" => AxisArrays.AxisArray(reshape([25000.0], 1), [1]))
    realized_bids = Dict("summer" => Dict("Gen1" => 1.0, "Gen2" => 0.4),
                         "winter" => Dict("Gen1" => 0.5))

    @testset "realized market data: two seasons of price and accepted bids survive" begin
        tmpfile = tempname() * ".h5"
        try
            EMISAgentSimulation.save_realized_market_data(
                tmpfile, _realized_market_args(realized_price, realized_bids)...)
            loaded = EMISAgentSimulation.load_realized_market_data(tmpfile)

            cap = loaded["capacity_price"]
            @test Set(keys(cap)) == Set(["summer", "winter"])
            @test cap["summer"][1] == 15000.0
            @test cap["winter"][1] == 25000.0

            bids = loaded["capacity_accepted_bids"]
            @test bids == realized_bids
            # Shape expected by calculate_realized_profit / annual_updates.jl.
            @test bids isa Dict{String, Dict{String, Float64}}
            @test loaded["cet_achieved_ratio"] == 0.9
        finally
            isfile(tmpfile) && rm(tmpfile)
        end
    end

    @testset "realized market data: legacy flat price and bids load as \"annual\"" begin
        tmpfile = tempname() * ".h5"
        try
            EMISAgentSimulation.save_realized_market_data(
                tmpfile, _realized_market_args(realized_price, realized_bids)...)
            _rewrite_as_legacy!(tmpfile, "capacity_price",
                g -> EMISAgentSimulation.save_axisarray!(g, AxisArrays.AxisArray(reshape([7000.0], 1), [1])))
            _rewrite_as_legacy!(tmpfile, "capacity_accepted_bids",
                g -> EMISAgentSimulation._save_dict_str_float!(g, Dict("Gen1" => 0.75)))

            loaded = EMISAgentSimulation.load_realized_market_data(tmpfile)
            @test collect(keys(loaded["capacity_price"])) == ["annual"]
            @test loaded["capacity_price"]["annual"][1] == 7000.0
            @test loaded["capacity_accepted_bids"] == Dict("annual" => Dict("Gen1" => 0.75))
        finally
            isfile(tmpfile) && rm(tmpfile)
        end
    end
end
