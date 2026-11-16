using Test
using EMISAgentSimulation
using PowerSystems
using DataFrames
using CSV

const PROJECT_ROOT = normpath(joinpath(@__DIR__, ".."))
const TECHNOLOGIES_FILE = joinpath(
    PROJECT_ROOT, "config", "ercot_est", "system_config", "technologies.csv"
)
const MAPPING_FILE = joinpath(
    PROJECT_ROOT, "config", "psy5", "psy_classification_mapping.csv"
)
const DEFAULTS_FILE = joinpath(PROJECT_ROOT, "config", "project_defaults.csv")
const OPTIONS_TEMPLATE = joinpath(
    PROJECT_ROOT, "config", "project_templates", "project_spec", "projectoptions.csv"
)
const EXISTING_TEMPLATE = joinpath(
    PROJECT_ROOT, "config", "project_templates", "project_spec", "projectexisting.csv"
)

@testset "Project input validation" begin
    mapping = load_psy_classification_mapping(MAPPING_FILE)
    technologies = DataFrame(CSV.File(TECHNOLOGIES_FILE))

    @test validate_psy_classification_mapping(mapping, technologies)
    @test nrow(load_project_defaults(DEFAULTS_FILE)) > 0
    @test nrow(validate_project_input_template(
        OPTIONS_TEMPLATE;
        kind=:options,
        investors=["investor_1"],
    )) == 1
    @test nrow(validate_project_input_template(
        EXISTING_TEMPLATE;
        kind=:existing,
        investors=["investor_1"],
    )) == 1

    invalid_mapping = copy(mapping)
    invalid_mapping.unit_type[1] = "UNKNOWN"
    @test_throws ErrorException validate_psy_classification_mapping(
        invalid_mapping, technologies
    )

    invalid_options = DataFrame(
        [["investor_1"], ["new_WT_2"], ["WT"], ["L"], [missing], [missing]],
        [:Investor, :GEN_UID, Symbol("Unit Type"), :Size, :Zone, Symbol("Bus ID")],
    )
    invalid_options_file = joinpath(mktempdir(), "projectoptions.csv")
    CSV.write(invalid_options_file, invalid_options)
    @test_throws ErrorException validate_project_input_template(
        invalid_options_file;
        kind=:options,
        investors=["investor_1"],
    )

    empty_system = PowerSystems.System(100.0; runchecks=false)
    @test_throws ErrorException extract_zones(empty_system)

    existing_projects = DataFrame(
        GEN_UID=["old_ST", "old_BA"],
        var"Online Year"=[2000, 2000],
        Lifetime=[55, 15],
    )
    @test validate_existing_project_lifetimes(existing_projects[1:1, :], 2020, "projectexisting.csv")
    @test_throws ErrorException validate_existing_project_lifetimes(existing_projects, 2020, "projectexisting.csv")
    existing_projects[2, "Online Year"] = 2015
    @test validate_existing_project_lifetimes(existing_projects, 2020, "projectexisting.csv")
    @test isempty(extract_branches(empty_system).ac)
    @test isempty(extract_branches(empty_system).dc)
    @test isempty(extract_reserves(empty_system))

    # "REG_DN"/"REG_UP" are the raw PSY reserve service names, while RTS_Data time-series
    # files use "Reg_Down"/"Reg_Up" (matching the constructed-system renaming in
    # src/struct_creators/simulation_structs/rts_psy_creator.jl). Neither the PSY system
    # nor the time-series files should be renamed to bridge this; extract_reserves should
    # resolve it via the known alias table.
    available_products = Dict("reg_down" => "Reg_Down", "spin" => "Spin")
    @test EMISAgentSimulation._resolve_reserve_product_name("REG_DN", available_products) == "Reg_Down"
    @test EMISAgentSimulation._resolve_reserve_product_name("SPIN", available_products) == "Spin"
    @test EMISAgentSimulation._resolve_reserve_product_name("NONSPIN", available_products) === nothing
    @test EMISAgentSimulation._resolve_reserve_product_name("REG_UP", available_products) === nothing

    reserve_defaults = DataFrame(
        Symbol.(EMISAgentSimulation.RESERVE_COLUMNS) .=> [
            ["Curve"], [60], [12.0], ["(1)"], ["(Generator)"], [""], ["Up"]
        ],
    )
    demand_curve = PowerSystems.ReserveDemandCurve{PowerSystems.ReserveUp}(nothing)
    @test ismissing(EMISAgentSimulation._reserve_requirement(
        demand_curve,
        nothing,
        "Curve",
    ))
    @test EMISAgentSimulation._reserve_requirement(
        demand_curve,
        reserve_defaults,
        "Curve",
    ) == 12.0
    ownership = DataFrame(Investor=String[], GEN_UID=String[])
    technologies = DataFrame(CSV.File(TECHNOLOGIES_FILE))
    @test nrow(extract_fleet(
        empty_system,
        mapping,
        technologies,
        ownership;
        defaults=load_project_defaults(DEFAULTS_FILE),
    ).projects) == 0
    output_dir = mktempdir()
    @test_throws ErrorException write_system_inputs(
        empty_system,
        output_dir;
        mapping=mapping,
        technologies=technologies,
    )

    spec_dir = joinpath(PROJECT_ROOT, "config", "project_templates", "project_spec")
    init_dir = joinpath(mktempdir(), "project_init_test")
    result = initialize_emis_project(spec_dir; output_dir=init_dir, reference_case_dir=nothing)
    @test isdir(joinpath(init_dir, "EMIS_RTS_Analysis", "Heterogeneous", "system_config"))
    @test isdir(joinpath(init_dir, "EMIS_RTS_Analysis", "Heterogeneous", "markets_data"))
    @test isdir(joinpath(init_dir, "EMIS_RTS_Analysis", "Heterogeneous", "investors", "investor_1", "markets_data"))
    @test isfile(joinpath(
        init_dir,
        "EMIS_RTS_Analysis",
        "Heterogeneous",
        "investors",
        "investor_1",
        "markets_data",
        "investor_belief.csv",
    ))
    @test isfile(joinpath(
        init_dir,
        "EMIS_RTS_Analysis",
        "Heterogeneous",
        "investors",
        "investor_2",
        "markets_data",
        "scenario_data.csv",
    ))
    @test haskey(result, :base_dir)
    @test isdir(joinpath(init_dir, "case_templates"))

    project_defaults = EMISAgentSimulation._default_lookup(
        load_project_defaults(DEFAULTS_FILE),
    )
    thermal_unit = "ST"
    output_points = [
        EMISAgentSimulation._option_default(
            project_defaults,
            thermal_unit,
            "Output_pct_$(i)",
            "NA",
        )
        for i in 0:4
    ]
    @test parse.(Float64, output_points[1:2]) == [0.0, 1.0]
    @test all(==("NA"), output_points[3:5])

    stale_root = mktempdir()
    stale_base = joinpath(stale_root, "EMIS_RTS_Analysis")
    mkpath(joinpath(stale_base, "case_1"))
    @test_throws ErrorException initialize_emis_project(
        spec_dir;
        output_dir=stale_root,
        reference_case_dir=nothing,
    )

    renamed_system_dir = mktempdir()
    renamed_system = joinpath(renamed_system_dir, "renamed_system.json")
    touch(renamed_system)
    @test resolve_generated_system_path(renamed_system_dir) == renamed_system

    investor_dir = mktempdir()
    mkpath(joinpath(investor_dir, "markets_data"))
    @test_throws ErrorException EMISAgentSimulation._validate_investor_market_bundle(investor_dir)
end

@testset "Cost-curve check uses technologies.csv thermal class" begin
    options = DataFrame(
        "Unit Type" => ["MY_GAS"],
        "Output_pct_0" => ["0.4"],
        "HR_avg_0" => ["NA"],   # incomplete curve point
    )
    path = "projectoptions.csv"
    custom = DataFrame(unit_type=["MY_GAS"], class=["thermal"])
    @test_throws r"incomplete curve point" EMISAgentSimulation.validate_project_option_cost_curve(options, path, custom)

    not_thermal = DataFrame(unit_type=["MY_GAS"], class=["renewable"])
    @test EMISAgentSimulation.validate_project_option_cost_curve(options, path, not_thermal) === nothing
end

@testset "gen.csv PMax equals projectexisting Size for wind/PV (system-base units)" begin
    # Regression: _component_capacity_mw returned get_rating in system per unit for RenewableGen
    # (no active_power_limits), so gen.csv "PMax MW" was 100x smaller than Size and the
    # investor availability (profile / PMax) about 100x too high.
    sys = PowerSystems.System(100.0; runchecks=false)
    area = PowerSystems.Area(; name="FarWest")
    PowerSystems.add_component!(sys, area)
    bus = PowerSystems.ACBus(; number=1, name="bus1", available=true, bustype=PowerSystems.ACBusTypes.REF,
        angle=0.0, magnitude=1.0, voltage_limits=(min=0.9, max=1.1), base_voltage=230.0, area=area)
    PowerSystems.add_component!(sys, bus)
    for (name, pm) in (("wind_1", PowerSystems.PrimeMovers.WT), ("pv_1", PowerSystems.PrimeMovers.PVe))
        gen = PowerSystems.RenewableDispatch(; name=name, available=true, bus=bus, active_power=0.0,
            reactive_power=0.0, rating=0.5, prime_mover_type=pm, reactive_power_limits=nothing,
            power_factor=1.0, operation_cost=PowerSystems.RenewableGenerationCost(nothing), base_power=120.0)
        PowerSystems.add_component!(sys, gen)   # 0.5 pu on a 120 MW device base = 60 MW
    end
    mapping = load_psy_classification_mapping(MAPPING_FILE)
    technologies = DataFrame(CSV.File(TECHNOLOGIES_FILE))
    gen = extract_gen_table(sys, mapping; technologies=technologies)
    ownership = DataFrame(Investor=["investor1", "investor1"], GEN_UID=["wind_1", "pv_1"])
    fleet = extract_fleet(sys, mapping, technologies, ownership;
        defaults=load_project_defaults(DEFAULTS_FILE)).projects
    for name in ("wind_1", "pv_1")
        pmax = only(gen[gen[!, "GEN UID"] .== name, "PMax MW"])
        size = only(fleet[fleet.GEN_UID .== name, "Size"])
        @test pmax ≈ 60.0
        @test size ≈ 60.0
        @test pmax ≈ size
    end
end

@testset "per-unit Lifetime and Online Year overrides in projectexisting ownership" begin
    # extract_fleet takes Lifetime from project_defaults.csv per unit type unless the ownership input
    # supplies a per-unit value (PER_UNIT_OVERRIDE_COLUMNS); blank cells fall back to the default.
    sys = PowerSystems.System(100.0; runchecks=false)
    area = PowerSystems.Area(; name="FarWest")
    PowerSystems.add_component!(sys, area)
    bus = PowerSystems.ACBus(; number=1, name="bus1", available=true, bustype=PowerSystems.ACBusTypes.REF,
        angle=0.0, magnitude=1.0, voltage_limits=(min=0.9, max=1.1), base_voltage=230.0, area=area)
    PowerSystems.add_component!(sys, bus)
    for name in ("wind_a", "wind_b")
        PowerSystems.add_component!(sys, PowerSystems.RenewableDispatch(; name=name, available=true, bus=bus,
            active_power=0.0, reactive_power=0.0, rating=1.0, prime_mover_type=PowerSystems.PrimeMovers.WT,
            reactive_power_limits=nothing, power_factor=1.0,
            operation_cost=PowerSystems.RenewableGenerationCost(nothing), base_power=50.0))
    end
    num(x) = x isa Number ? Float64(x) : parse(Float64, strip(string(x)))
    mapping = load_psy_classification_mapping(MAPPING_FILE)
    technologies = DataFrame(CSV.File(TECHNOLOGIES_FILE))
    defaults = load_project_defaults(DEFAULTS_FILE)
    default_lifetime = num(only(defaults[(defaults.unit_type .== "WT") .& (defaults.field .== "Lifetime"), :value]))
    ownership = DataFrame(Investor=["investor1", "investor1"], GEN_UID=["wind_a", "wind_b"],
        var"Online Year"=["2005", ""], Lifetime=["31", ""])
    fleet = extract_fleet(sys, mapping, technologies, ownership; defaults=defaults).projects
    row(n) = fleet[only(findall(==(n), fleet.GEN_UID)), :]
    @test num(row("wind_a").Lifetime) == 31.0
    @test num(row("wind_a")[Symbol("Online Year")]) == 2005.0
    # whole numbers stay integers (an Int64 Online Year is required by calculate_capcost_and_wacc,
    # and a Float64 would be written to projectexisting.csv as "2005.0")
    @test row("wind_a")[Symbol("Online Year")] === 2005
    @test row("wind_a").Lifetime === 31
    @test EMISAgentSimulation._ownership_override((var"Online Year"="2005.5",), "Online Year") === 2005.5
    @test num(row("wind_b").Lifetime) == default_lifetime
    # without the column the default applies to every unit
    fleet2 = extract_fleet(sys, mapping, technologies, ownership[:, [:Investor, :GEN_UID]]; defaults=defaults).projects
    @test all(num.(fleet2.Lifetime) .== default_lifetime)
end
