using Test
using DataFrames
using CSV
using EMISAgentSimulation

@testset "SystemConfig" begin
    tmpdir = mktempdir()
    config_dir = joinpath(tmpdir, "system_config")
    mkpath(config_dir)

    DataFrames.DataFrame(
        zone_id = [1, 2],
        zone_name = ["zone_a", "zone_b"],
        area_name = ["Area A", "Area B"],
        load_column = ["load_zone_1", "load_zone_2"],
    ) |> df -> CSV.write(joinpath(config_dir, "zones.csv"), df)

    DataFrames.DataFrame(
        scenario_name = ["scenario_1", "scenario_2"],
        probability = [0.8, 0.2],
        pcm_label = ["baseline", "central"],
        weather_year = [2020, 2021],
    ) |> df -> CSV.write(joinpath(config_dir, "scenarios.csv"), df)

    DataFrames.DataFrame(
        unit_type = ["WT", "CC"],
        category = ["Wind", "Gas CC"],
        class = ["renewable", "thermal"],
        capacity_eligible = [true, true],
        mopr_exempt = [true, false],
        macrs_years = [20, 25],
        duration_hr = [missing, missing],
    ) |> df -> CSV.write(joinpath(config_dir, "technologies.csv"), df)

    DataFrames.DataFrame(
        device_type = ["ThermalStandard"],
        device_name = ["AUSTIN_1"],
    ) |> df -> CSV.write(joinpath(config_dir, "devices_to_remove.csv"), df)

    DataFrames.DataFrame(
        key = ["default_rts_load", "base_load_year", "zone_column_start"],
        value = [90.0, 2020, 6],
    ) |> df -> CSV.write(joinpath(config_dir, "system_config.csv"), df)

    cfg = load_system_config(tmpdir)
    @test cfg.zone_names == ["zone_a", "zone_b"]
    @test get_zone_name(cfg, 1) == "zone_a"
    @test get_zone_name(cfg, 2) == "zone_b"
    @test get_zone_name(cfg, "Area A") == "zone_a"
    @test get_zone_name(cfg, "load_zone_2") == "zone_b"
    @test cfg.scenario_names == ["scenario_1", "scenario_2"]
    @test get_default_scenario(cfg) == "scenario_1"
    @test cfg.default_rts_load == 90.0
    @test cfg.zone_column_start == 6
    @test EMISAgentSimulation.get_technology_class(cfg, "WT") == "renewable"
    @test EMISAgentSimulation.get_technology_class(cfg, "CC") == "thermal"
    @test EMISAgentSimulation.is_mopr_exempt(cfg, "WT")
    @test cfg.devices_to_remove["ThermalStandard"] == ["AUSTIN_1"]

    rm(joinpath(config_dir, "devices_to_remove.csv"))
    no_pruning_cfg = load_system_config(tmpdir)
    @test isempty(no_pruning_cfg.devices_to_remove)

    legacy_root = normpath(joinpath(@__DIR__, "..", "config", "ercot_est"))
    legacy = load_system_config(legacy_root)
    @test legacy.zone_names == ["zone_$(index)" for index in 1:8]
    # Format checks only: which area belongs to which zone is a fact about the data,
    # checked against the system in test_zone_consistency.jl, not restated here.
    legacy_zones = DataFrame(CSV.File(joinpath(legacy_root, "system_config", "zones.csv")))
    @test legacy_zones.zone_id == 1:8
    @test legacy_zones.zone_name == ["zone_$(id)" for id in legacy_zones.zone_id]
    @test legacy_zones.load_column == ["load_zone_$(id)" for id in legacy_zones.zone_id]
    @test allunique(legacy_zones.area_name)
    for row in eachrow(legacy_zones)
        @test get_zone_name(legacy, string(row.zone_id)) == row.zone_name
        @test get_zone_name(legacy, row.area_name) == row.zone_name
        @test get_zone_name(legacy, row.load_column) == row.zone_name
    end
    @test get_default_scenario(legacy) == "scenario_1"
    @test EMISAgentSimulation.get_scenario_pcm_label(legacy, "scenario_1") == "baseline"
    @test EMISAgentSimulation.get_scenario_pcm_label(legacy, "scenario_2") == "central"
    @test EMISAgentSimulation.get_scenario_pcm_label(legacy, "scenario_3") == "ira"
    @test legacy.default_rts_load == 75.0
    @test legacy.base_load_year == 2020
    @test_throws ErrorException load_system_config(joinpath(tmpdir, "missing"))
end
