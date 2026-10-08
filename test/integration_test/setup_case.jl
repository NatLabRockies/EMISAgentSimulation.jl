# Initialize the committed RTS-GMLC example spec into a fresh project, validate it, and stamp a case.
# Usage: julia --project=. test/integration_test/setup_case.jl [--spec-dir DIR] [--output-root DIR] [--scratch-dir DIR]
#            [--case-name NAME] [--force]
# Prints `CASE_DIR=<path>` on success so shell drivers can locate the rendered run script.

using EMISAgentSimulation
using CSV, DataFrames
const EAS = EMISAgentSimulation

const REPO_ROOT = normpath(joinpath(@__DIR__, "..", ".."))
const DEFAULT_SPEC_DIR = joinpath(REPO_ROOT, "config", "example_systems", "rts_gmlc", "project_spec")
const DEFAULT_SCRATCH_DIR = get(ENV, "EMIS_SCRATCH_DIR", joinpath("/scratch", get(ENV, "USER", "user")))
const DEFAULT_OUTPUT_ROOT = joinpath(DEFAULT_SCRATCH_DIR, "emis_rts_gmlc_integration")

function arg_value(flag::AbstractString, default)
    index = findfirst(==(flag), ARGS)
    index === nothing && return default
    index == length(ARGS) && error("Missing value after $(flag)")
    return ARGS[index + 1]
end

# Extracted existing projects carry PSY area names ("1","2","3"), which CSV reads back as Int;
# rewrite them to the zone_name form used by projectoptions.csv and the market files.
function normalize_existing_zones(spec_dir::AbstractString, base_dir::AbstractString)
    zones = CSV.read(joinpath(spec_dir, "zones.csv"), DataFrame; types = String)
    zone_name = Dict(zip(zones.area_name, zones.zone_name))
    for (dir, _, files) in walkdir(base_dir), file in files
        file == "projectexisting.csv" || continue
        path = joinpath(dir, file)
        df = CSV.read(path, DataFrame; types = Dict("Zone" => String))
        df[!, "Zone"] = [get(zone_name, z, z) for z in df.Zone]
        CSV.write(path, df)
    end
end

# The committed spec carries a placeholder scratch_dir; copy the spec so the caller's scratch directory is used.
function spec_with_scratch_dir(spec_dir::AbstractString, copy_dir::AbstractString, scratch_dir::AbstractString)
    mkpath(scratch_dir)
    rm(copy_dir; recursive = true, force = true)
    mkpath(dirname(copy_dir))
    cp(spec_dir, copy_dir)
    path = joinpath(copy_dir, "project_spec.csv")
    spec = CSV.read(path, DataFrame; types = String)
    spec[spec.key .== "scratch_dir", :value] .= abspath(scratch_dir)
    CSV.write(path, spec)
    return copy_dir
end

function main()
    spec_dir = arg_value("--spec-dir", DEFAULT_SPEC_DIR)
    output_root = arg_value("--output-root", DEFAULT_OUTPUT_ROOT)
    scratch_dir = arg_value("--scratch-dir", DEFAULT_SCRATCH_DIR)
    case_name = arg_value("--case-name", "rts_gmlc_2yr")
    project_dir = joinpath(output_root, "project")

    isfile(joinpath(spec_dir, "project_spec.csv")) || error(
        "RTS-GMLC spec not found at $(spec_dir); run scripts/rts_gmlc_example/build_spec.jl first",
    )
    if ispath(project_dir)
        "--force" in ARGS || error("$(project_dir) already exists; pass --force to recreate it")
        isfile(joinpath(project_dir, "project_metadata.csv")) ||
            error("Refusing to delete $(project_dir): it is not an initialized EMIS project")
        @info "Removing existing project" project_dir
        rm(project_dir; recursive = true)
    end
    spec_dir = spec_with_scratch_dir(spec_dir, joinpath(output_root, "project_spec"), scratch_dir)

    started = time()
    result = EAS.initialize_emis_project(spec_dir; output_dir = project_dir)
    normalize_existing_zones(spec_dir, result[:base_dir])
    EAS.validate_emis_project(project_dir)
    case = EAS.new_emis_case(project_dir, case_name)
    @info "RTS-GMLC case ready" project_dir case_dir = case[:case_dir] base_dir = result[:base_dir] seconds =
        round(time() - started; digits = 1)
    println("CASE_DIR=", case[:case_dir])
    return case
end

main()
