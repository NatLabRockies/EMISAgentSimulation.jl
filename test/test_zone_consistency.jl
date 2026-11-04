using Test
using EMISAgentSimulation
using CSV
using DataFrames
using PowerSystems

const ZONE_TEST_ROOT = normpath(joinpath(@__DIR__, ".."))
const PSY_ZONES = PowerSystems

# Every bundled zones.csv follows config/ercot_est/system_config/zones.csv:
# zone N = zone_id N, zone_name "zone_N", one PSY area, load_column "load_zone_N".
@testset "Bundled zones.csv files share one format" begin
    for path in (
        joinpath(ZONE_TEST_ROOT, "config", "ercot_est", "system_config", "zones.csv"),
        joinpath(ZONE_TEST_ROOT, "config", "example_systems", "rts_gmlc", "project_spec", "zones.csv"),
        joinpath(ZONE_TEST_ROOT, "config", "project_templates", "project_spec", "zones.csv"),
    )
        zones = DataFrame(CSV.File(path))
        @test names(zones) == ["zone_id", "zone_name", "area_name", "load_column"]
        @test zones.zone_id == 1:nrow(zones)
        @test zones.zone_name == ["zone_$(id)" for id in zones.zone_id]
        @test zones.load_column == ["load_zone_$(id)" for id in zones.zone_id]
        @test allunique(string.(zones.area_name))
    end
end

@testset "Branch ends map to zones through area_name" begin
    cfg = load_system_config(joinpath(ZONE_TEST_ROOT, "config", "ercot_est"))
    with_areas = DataFrame(
        "UID" => ["l1"], "From Bus" => ["8001"], "To Bus" => ["1001"],
        "From Area" => ["FarWest"], "To Area" => ["East"],
    )
    # Area columns win over the bus number's first digit.
    @test EMISAgentSimulation._branch_end_zone(cfg, with_areas, 1, "From") == "zone_1"
    @test EMISAgentSimulation._branch_end_zone(cfg, with_areas, 1, "To") == "zone_8"
    legacy = DataFrame("UID" => ["l1"], "From Bus" => [5001], "To Bus" => [7001])
    @test EMISAgentSimulation._branch_end_zone(cfg, legacy, 1, "From") == "zone_5"
    @test EMISAgentSimulation._branch_end_zone(cfg, legacy, 1, "To") == "zone_7"
end

@testset "Initialized RTS-GMLC project uses zones.csv zone names everywhere" begin
    spec_dir = joinpath(ZONE_TEST_ROOT, "config", "example_systems", "rts_gmlc", "project_spec")
    init_dir = joinpath(mktempdir(), "rts_zones")
    initialize_emis_project(spec_dir; output_dir=init_dir, reference_case_dir=nothing)
    metadata = EMISAgentSimulation._read_project_metadata(init_dir)
    config_root, investor_root, _ = EMISAgentSimulation._base_project_paths(init_dir)
    cfg = load_system_config(dirname(config_root))
    zones = DataFrame(CSV.File(joinpath(config_root, "zones.csv"); stringtype=String))
    sys = PSY_ZONES.System(joinpath(metadata["test_system_dir"], metadata["system_file"]); runchecks=false)
    source_dir = joinpath(metadata["test_system_dir"], "RTS_Data", "SourceData")

    # Every PSY area is a zone, and the default extraction produces the same format.
    @test sort(PSY_ZONES.get_name.(PSY_ZONES.get_components(PSY_ZONES.Area, sys))) ==
          sort(string.(zones.area_name))
    extracted = extract_zones(sys)
    @test extracted.zone_name == ["zone_$(id)" for id in extracted.zone_id]
    @test extracted.load_column == ["load_zone_$(id)" for id in extracted.zone_id]

    # No second, differently formatted zones.csv next to the source data.
    @test !isfile(joinpath(source_dir, "zones.csv"))

    # Existing and option projects are located by zone_name.
    for investor in readdir(investor_root), file in ("projectexisting.csv", "projectoptions.csv")
        path = joinpath(investor_root, investor, file)
        isfile(path) || continue
        projects = DataFrame(CSV.File(path; stringtype=String))
        "Zone" in names(projects) || continue
        @test all(in(cfg.zone_names), string.(projects.Zone))
    end

    # Branch tables carry areas, and every branch end resolves to a configured zone.
    branches = DataFrame(CSV.File(joinpath(source_dir, "branch.csv"); stringtype=String))
    @test all(in(names(branches)), ["From Area", "To Area"])
    for b in 1:nrow(branches), side in ("From", "To")
        @test EMISAgentSimulation._branch_end_zone(cfg, branches, b, side) in cfg.zone_names
    end

    # Market-side lookup: every zone finds its area; unknown zones fail loudly.
    for row in eachrow(zones)
        area = EMISAgentSimulation.find_zonal_area(row.zone_name, sys, cfg)
        @test PSY_ZONES.get_name(area) == string(row.area_name)
    end
    @test_throws ErrorException EMISAgentSimulation.find_zonal_area("zone_99", sys, cfg)
end
