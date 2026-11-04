"""
    read_generator_table(test_system_dir, data_dir)

Read `<test_system_dir>/RTS_Data/SourceData/gen.csv` with every `Unit Type` expressed as a
`technologies.csv` unit type. Types already in `technologies.csv` are kept; others (e.g. the
raw names in a legacy `gen.csv`) are converted through `unit_type_aliases.csv`; any type
left unknown is an error.
"""
function read_generator_table(test_system_dir::AbstractString, data_dir::AbstractString)
    path = joinpath(test_system_dir, "RTS_Data", "SourceData", "gen.csv")
    generators = DataFrames.DataFrame(CSV.File(path))
    system_config = load_system_config(data_dir)
    aliases = load_unit_type_aliases(data_dir)
    known(unit_type) = haskey(system_config.technology_lookup, unit_type)
    unit_types = map(generators[!, "Unit Type"]) do value
        unit_type = _coerce_string(value)
        known(unit_type) ? unit_type : get(aliases, unit_type, unit_type)
    end
    unknown = sort(unique(filter(!known, unit_types)))
    isempty(unknown) || error(
        "$(path) has unit type(s) $(join(unknown, ", ")) that are not in technologies.csv " *
        "and have no row in unit_type_aliases.csv",
    )
    generators[!, "Unit Type"] = unit_types
    return generators
end

"""
This function reads the test system time series and returns representative hour weight.
"""
function read_test_system(data_dir::String,
    test_system_dir::String,
    base_dir::String,
    scenario::String,
    test_system_load_da::DataFrames.DataFrame,
    test_system_load_rt::DataFrames.DataFrame,
    base_year::Int64,
    annual_growth_past::AxisArrays.AxisArray{Float64, 2},
    start_year::Int64,
    sim_year::Int64,
    rep_period_interval::Int64,
    n_rep_periods::Int64,
    rep_checkpoint::Int64,
    timeseries_data_dir::String)
    test_sys_hour_weight = nothing
    zones = nothing
    representative_periods = nothing
    chron_weights = nothing
    rep_hour_weight = nothing
    system_peak_load = nothing
    test_sys_hour_weight = nothing
    zonal_lines = nothing

    if isdir(joinpath(test_system_dir, "RTS_Data"))
        zones,
        representative_periods,
        rep_hour_weight,
        chron_weights,
        system_peak_load,
        test_sys_hour_weight,
        zonal_lines = read_rts(data_dir,
            test_system_dir,
            base_dir,
            scenario,
            test_system_load_da,
            test_system_load_rt,
            base_year,
            annual_growth_past,
            start_year,
            sim_year,
            rep_period_interval,
            n_rep_periods,
            rep_checkpoint,
            timeseries_data_dir)
    else
        error("Test system directory does not contain RTS_Data: $(test_system_dir)")
    end

    return zones,
    representative_periods,
    rep_hour_weight,
    chron_weights,
    system_peak_load,
    test_sys_hour_weight,
    zonal_lines
end
