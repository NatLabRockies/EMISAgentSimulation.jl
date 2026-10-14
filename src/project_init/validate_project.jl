function _project_csv_file(project_root::AbstractString, filename::AbstractString)
    path = joinpath(project_root, filename)
    isfile(path) || error("Missing project input: $(path)")
    return path
end

function _read_csv_df(path::AbstractString)
    isfile(path) || error("CSV missing: $(path)")
    return DataFrames.DataFrame(CSV.File(path; stringtype=String))
end

function _as_string(value)
    return strip(string(value))
end

function _must_have_column(df::DataFrames.DataFrame, column::Symbol, path::AbstractString)
    column in Symbol.(names(df)) || error("$(path) is missing required column $(column)")
    return nothing
end

function _base_project_paths(project_root::AbstractString)
    if isfile(joinpath(project_root, "project_metadata.csv"))
        metadata = _read_project_metadata(project_root)
        base_dir = get(metadata, "base_dir", project_root)
        heterogeneity = get(metadata, "heterogeneity", "Heterogeneous")
        config_root = joinpath(base_dir, heterogeneity, "system_config")
        investor_root = joinpath(base_dir, heterogeneity, "investors")
        ts_root = get(metadata, "timeseries_data_dir", joinpath(project_root, "timeseries"))
        return config_root, investor_root, ts_root
    end
    return project_root, joinpath(project_root, "investors"), joinpath(project_root, "timeseries")
end

function _project_zone_names(config_root::AbstractString)
    zones_path = joinpath(config_root, "zones.csv")
    isfile(zones_path) || error("Missing zones.csv in $(config_root)")
    zones = _read_csv_df(zones_path)
    _must_have_column(zones, :zone_name, zones_path)
    return Set(_as_string.(zones.zone_name))
end

function _project_scenario_names(config_root::AbstractString)
    scenarios_path = joinpath(config_root, "scenarios.csv")
    isfile(scenarios_path) || error("Missing scenarios.csv in $(config_root)")
    scenarios = _read_csv_df(scenarios_path)
    _must_have_column(scenarios, :scenario_name, scenarios_path)
    return Set(_as_string.(scenarios.scenario_name))
end

function _project_technology_names(config_root::AbstractString)
    tech_path = joinpath(config_root, "technologies.csv")
    isfile(tech_path) || error("Missing technologies.csv in $(config_root)")
    techs = _read_csv_df(tech_path)
    _must_have_column(techs, :unit_type, tech_path)
    return Set(_as_string.(techs.unit_type))
end

function _validate_probability_sum(config_root::AbstractString)
    scenarios_path = joinpath(config_root, "scenarios.csv")
    scenarios = _read_csv_df(scenarios_path)
    _must_have_column(scenarios, :probability, scenarios_path)
    probabilities = Float64[]
    for row in eachrow(scenarios)
        val = _as_string(row.probability)
        isempty(val) && error("Scenario probability is empty in $(scenarios_path)")
        push!(probabilities, parse(Float64, val))
    end
    prob_total = sum(probabilities)
    if abs(prob_total - 1.0) < 1e-8 || all(probability -> abs(probability - 1.0) < 1e-8, probabilities)
        return nothing
    end
    error("Scenario probabilities sum to $(prob_total), expected 1.0 or one scenario alternative per row")
    return nothing
end

"""
Errors if a regional Load csv has a zone column that `zones.csv` cannot resolve, or if a
zone has no column. Columns are resolved through the same `SystemConfig` alias lookup
(`zone_id`/`area_name`/`load_column`/position) the runtime uses in
`_canonical_zone_load_frame`, so this checks the file exactly as the simulation reads it.
"""
function _validate_load_zone_columns(config_root::AbstractString, ts_root::AbstractString)
    isdir(ts_root) || return nothing
    cfg = load_system_config(dirname(config_root))

    valid_scenarios = _project_scenario_names(config_root)
    for scenario in readdir(ts_root)
        scenario_path = joinpath(ts_root, scenario)
        isdir(scenario_path) || continue
        if !(scenario in valid_scenarios)
            error("Timeseries scenario folder $(scenario) is not present in scenarios.csv")
        end
        for sim_year in readdir(scenario_path)
            year_path = joinpath(scenario_path, sim_year)
            isdir(year_path) || continue
            load_dir = joinpath(year_path, "Load")
            isdir(load_dir) || continue
            for filename in ("DAY_AHEAD_regional_Load.csv", "REAL_TIME_regional_Load.csv")
                load_file = joinpath(load_dir, filename)
                isfile(load_file) || continue
                columns = _as_string.(names(_read_csv_df(load_file))[cfg.zone_column_start:end])
                resolved = Set{String}()
                for column in columns
                    haskey(cfg.zone_lookup, column) ||
                        error("$(load_file) has load column $(column) that matches no zone in zones.csv")
                    push!(resolved, cfg.zone_lookup[column])
                end
                missing_zones = sort(setdiff(cfg.zone_names, resolved))
                isempty(missing_zones) ||
                    error("$(load_file) has no load column for zone(s): $(join(missing_zones, ", "))")
            end
        end
    end
    return nothing
end

function _validate_option_techs(config_root::AbstractString, investor_root::AbstractString)
    valid_techs = _project_technology_names(config_root)
    isdir(investor_root) || return nothing
    for investor in readdir(investor_root)
        investor_path = joinpath(investor_root, investor)
        isdir(investor_path) || continue
        options_path = joinpath(investor_path, "projectoptions.csv")
        if !isfile(options_path)
            continue
        end
        options = _read_csv_df(options_path)
        if Symbol("Unit Type") in names(options)
            for row in eachrow(options)
                tech = _as_string(row[Symbol("Unit Type")])
                isempty(tech) && continue
                if !(tech in valid_techs)
                    error("Investor $(investor) projectoptions.csv references unknown technology $(tech)")
                end
            end
        end
    end
    return nothing
end

function _csv_category_set(path::AbstractString)
    isfile(path) || return nothing
    df = _read_csv_df(path)
    :Category in Symbol.(names(df)) || error("$(path) is missing required column Category")
    return Set(_as_string.(df.Category))
end

function _investor_used_categories(investor_dir::AbstractString)
    categories = Set{String}()
    for filename in ("projectexisting.csv", "projectoptions.csv")
        path = joinpath(investor_dir, filename)
        isfile(path) || continue
        found = _csv_category_set(path)
        found === nothing || union!(categories, found)
    end
    return categories
end

function _validate_investor_finance_categories(investor_dir::AbstractString)
    investor = basename(investor_dir)
    categories = _investor_used_categories(investor_dir)
    isempty(categories) && return nothing
    for filename in ("finance_params.csv", "project_capex.csv")
        path = joinpath(investor_dir, filename)
        # Finance/capex files are optional (Tier 3); only cross-check categories when present.
        isfile(path) || continue
        covered = _csv_category_set(path)
        missing_categories = setdiff(categories, covered)
        isempty(missing_categories) || error(
            "Investor $(investor)'s $(filename) is missing Category row(s) $(join(sort(collect(missing_categories)), ", ")) " *
            "used by its own projectexisting.csv/projectoptions.csv; add matching rows or fix the technologies.csv unit_type→category mapping"
        )
    end
    return nothing
end

function _validate_technology_finance_coverage(config_root::AbstractString, investor_root::AbstractString)
    isdir(investor_root) || return nothing
    for investor in readdir(investor_root)
        investor_path = joinpath(investor_root, investor)
        isdir(investor_path) || continue
        _validate_investor_finance_categories(investor_path)
    end
    return nothing
end

function _validate_investor_market_bundle(investor_dir::AbstractString)
    isdir(investor_dir) || error("Investor directory does not exist: $(investor_dir)")
    markets_dir = joinpath(investor_dir, "markets_data")
    isdir(markets_dir) || error("Investor $(basename(investor_dir)) is missing its markets_data directory")
    required_files = ["investor_belief.csv", "scenario_data.csv"]
    missing = [file for file in required_files if !isfile(joinpath(markets_dir, file))]
    isempty(missing) || error(
        "Investor $(basename(investor_dir)) is missing required forecast bundle files: $(join(missing, ", "))"
    )
    return nothing
end

function _validate_project_investor_bundles(investor_root::AbstractString)
    isdir(investor_root) || return nothing
    for investor in readdir(investor_root)
        investor_path = joinpath(investor_root, investor)
        isdir(investor_path) || continue
        _validate_investor_market_bundle(investor_path)
    end
    return nothing
end

"""
Errors if a system-level `markets_data/` file read unconditionally at runtime is missing,
or if `Energy.csv` lacks a `price_cap` for any project zone.
"""
function _validate_system_market_files(config_root::AbstractString, investor_root::AbstractString)
    markets_dir = joinpath(dirname(investor_root), "markets_data")
    for filename in ("Energy.csv", "reserve_products.csv", "annual_growth.csv")
        isfile(joinpath(markets_dir, filename)) ||
            error("Missing system-level market input: $(joinpath(markets_dir, filename))")
    end
    path = joinpath(markets_dir, "Energy.csv")
    energy = _read_csv_df(path)
    _must_have_column(energy, :zones, path)
    _must_have_column(energy, :price_cap, path)
    missing_zones = sort(collect(setdiff(_project_zone_names(config_root), Set(_as_string.(energy.zones)))))
    isempty(missing_zones) || error(
        "$(path) is missing price_cap for zone(s): $(join(missing_zones, ", "))"
    )
    return nothing
end

"""
Errors if a wind (WT) or PV (PVe) generator in the project's PSY system has no column in
its scenario's DA/RT `WIND`/`PV` csv. The runtime (`rts_psy_creator.jl`) only warns and
leaves such a generator without a time series. Extra columns are allowed, since option
units are not in the base system. Skipped when the project has no system file.
"""
function _validate_psy_generator_names(project_root::AbstractString, ts_root::AbstractString)
    isdir(ts_root) || return nothing
    isfile(joinpath(project_root, "project_metadata.csv")) || return nothing
    metadata = _read_project_metadata(project_root)
    system_file = get(metadata, "system_file", "")
    isempty(system_file) && return nothing
    system_path = joinpath(metadata["test_system_dir"], system_file)
    isfile(system_path) || error("Project system file not found: $(system_path)")
    sys = PSY.System(system_path; runchecks=false)
    generator_names(prime_mover) = [
        PSY.get_name(gen) for gen in
        PSY.get_components(gen -> PSY.get_prime_mover_type(gen) == prime_mover, PSY.Generator, sys)
    ]
    vre_files = (
        ("WIND", "wind", generator_names(PSY.PrimeMovers.WT)),
        ("PV", "pv", generator_names(PSY.PrimeMovers.PVe)),
    )
    for scenario in readdir(ts_root)
        scenario_path = joinpath(ts_root, scenario)
        isdir(scenario_path) || continue
        for sim_year in readdir(scenario_path)
            year_path = joinpath(scenario_path, sim_year)
            isdir(year_path) || continue
            for (kind, suffix, generators) in vre_files, stage in ("DAY_AHEAD", "REAL_TIME")
                isempty(generators) && continue
                path = joinpath(year_path, kind, "$(stage)_$(suffix).csv")
                isfile(path) || error("Missing $(kind) time series file: $(path)")
                missing_generators = sort(setdiff(generators, names(_read_csv_df(path))))
                isempty(missing_generators) || error(
                    "$(path) has no column for $(kind) generator(s): $(join(missing_generators, ", "))"
                )
            end
        end
    end
    return nothing
end

function _required_derating_columns(config_root::AbstractString, investor_root::AbstractString)
    tech_path = joinpath(config_root, "technologies.csv")
    techs = _read_csv_df(tech_path)
    required = Dict{String, String}()
    ("class" in names(techs) && isdir(investor_root)) || return required
    tech_class = Dict(_as_string(row.unit_type) => _as_string(row.class) for row in eachrow(techs))
    zones = _read_csv_df(joinpath(config_root, "zones.csv"))
    zone_lookup = _build_zone_lookup(String.(zones.zone_name), zones)
    for investor in readdir(investor_root), (filename, prefix) in
        (("projectexisting.csv", "existing"), ("projectoptions.csv", "new"))
        path = joinpath(investor_root, investor, filename)
        isfile(path) || continue
        projects = _read_csv_df(path)
        all(in(names(projects)), ("GEN_UID", "Unit Type", "Zone")) || continue
        for row in eachrow(projects)
            unit_type = _as_string(row["Unit Type"])
            class = get(tech_class, unit_type, "")
            if class == "renewable"
                zone = _as_string(row.Zone)
                column = "$(prefix)_$(unit_type)_$(get(zone_lookup, zone, zone))"
            elseif class == "storage" && "Duration Hr" in names(projects)
                duration = tryparse(Float64, _as_string(row["Duration Hr"]))
                duration === nothing && error("$(path): storage project $(row.GEN_UID) has non-numeric Duration Hr")
                column = "$(prefix)_STOR_$(Int(round(duration)))"
            else
                continue
            end
            get!(required, column, "$(investor)/$(filename):$(row.GEN_UID)")
        end
    end
    return required
end

"""
Errors if a scenario has no `derating_dict.csv`, or if it lacks an
`existing_/new_<type>_<zone>` or `existing_/new_STOR_<d>` column that the derating
calculator assigns for a renewable or storage project.
"""
function _validate_derating_columns(config_root::AbstractString, investor_root::AbstractString)
    required = _required_derating_columns(config_root, investor_root)
    derating_root = joinpath(dirname(investor_root), "markets_data", "derating_data")
    for scenario in sort(collect(_project_scenario_names(config_root)))
        path = joinpath(derating_root, scenario, "derating_dict.csv")
        isfile(path) || error("Missing derating template: $(path)")
        isempty(required) && continue
        available = Set(names(_read_csv_df(path)))
        missing_columns = sort([column for column in keys(required) if !(column in available)])
        isempty(missing_columns) || error(
            "$(path) is missing derating column(s) required by project(s): " *
            join(["$(column) ($(required[column]))" for column in missing_columns], ", ")
        )
    end
    return nothing
end

function validate_emis_project(project_root::AbstractString)
    isdir(project_root) || error("Project root does not exist: $(project_root)")

    config_root, investor_root, ts_root = _base_project_paths(project_root)

    for filename in ("zones.csv", "technologies.csv", "scenarios.csv")
        _project_csv_file(config_root, filename)
    end
    if isfile(joinpath(project_root, "system_config.csv"))
        _project_csv_file(project_root, "system_config.csv")
    elseif isfile(joinpath(config_root, "system_config.csv"))
        _project_csv_file(config_root, "system_config.csv")
    end

    _validate_probability_sum(config_root)
    _validate_load_zone_columns(config_root, ts_root)
    _validate_option_techs(config_root, investor_root)
    _validate_technology_finance_coverage(config_root, investor_root)
    _validate_project_investor_bundles(investor_root)
    _validate_system_market_files(config_root, investor_root)
    _validate_psy_generator_names(project_root, ts_root)
    _validate_derating_columns(config_root, investor_root)

    return project_root
end
