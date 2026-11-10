using Test
using Logging
using EMISAgentSimulation
using DataFrames
using CSV
using AxisArrays

# Self-contained tests — run without any external setup.
const SELF_CONTAINED_TESTS = [
    "test_PSI.jl",
    "test_hdf5_serialization.jl",
    "test_read_cc_scalar.jl",
    "test_update_derating_factor.jl",
    "test_system_config.jl",
    "test_project_input_validation.jl",
    "test_project_init.jl",
    "test_constructed_systems_generator.jl",
    "test_zone_consistency.jl",
]

# `julia --project=test test/runtests.jl test_project_init.jl ...` runs only the named files.
for file in (isempty(ARGS) ? SELF_CONTAINED_TESTS : ARGS)
    include(file)
end

# Context-dependent tests — require the caller to have defined:
#   case, simulation_years, test_system_dir  (for test_base_checks.jl)
#   projectdata, investor_dir                (for test_system_inputs.jl)
if isempty(ARGS)
    if isdefined(Main, :case)
        include("test_base_checks.jl")
    else
        @info "Skipping test_base_checks.jl: `case` is not defined"
    end
    if isdefined(Main, :projectdata)
        include("test_system_inputs.jl")
    else
        @info "Skipping test_system_inputs.jl: `projectdata` is not defined"
    end
end
