"""
This function returns a dataframe of given csv data.
"""
function read_data(file_name::String)
    for attempt in 1:5
        isfile(file_name) && break
        @warn "File not yet visible (attempt $attempt/5): $file_name"
        sleep(2)
    end
    projectdata = DataFrames.DataFrame(
        CSV.File(file_name;
            truestrings = ["T", "TRUE", "true"],
            falsestrings = ["F", "FALSE", "false"]),
    );

    return projectdata
end

function write_data(dir::String, file_name::String, data::DataFrames.DataFrame)
    dir_exists(dir::String)
    CSV.write(joinpath(dir, file_name), data)

    return
end

"""
This function makes the directory specified in the argument, if it doesn't exist.
Returns nothing.
"""
function dir_exists(dir::String)
    try
        readdir(dir)
    catch err
        mkpath(dir)
    end
    return
end

"""
    resolve_generated_system_path(base_dir::AbstractString)

Resolve the canonical PSY system JSON for a generated project or fall back to the
legacy RTS bundle when no generated-system name is available.

The runtime owns this contract; project_init-only code should not define system
resolution helpers that the main simulation stack depends on.
"""
function resolve_generated_system_path(base_dir::AbstractString)
    isdir(base_dir) || error("System directory does not exist: $(base_dir)")

    json_candidates = String[]
    for entry in readdir(base_dir)
        path = joinpath(base_dir, entry)
        isfile(path) || continue
        lowercase(splitext(entry)[2]) == ".json" && push!(json_candidates, path)
    end
    if !isempty(json_candidates)
        return sort(json_candidates)[1]
    end

    legacy_path = joinpath(base_dir, "DA_sys_zonal_with_storage_capacities.json")
    isfile(legacy_path) && return legacy_path

    error(
        "No PSY system JSON was found in $(base_dir). Expected a generated project " *
        "copy or the legacy RTS file."
    )
end

