import JSON
import PowerSimulations: SimulationPartitions, get_num_partitions

const RUN_SCRIPT = "run_RTS_UC-ED2.jl"

function configure_parallel_simulation(
    script::AbstractString,
    num_steps::Integer,
    num_period_steps::Integer,
    output_dir::AbstractString;
    num_overlap_steps::Integer = 1,
    project_path = ".",
    simulation_name = "simulation",
)
    partitions = SimulationPartitions(num_steps, num_period_steps, num_overlap_steps)
    julia_cmd = isnothing(project_path) ? "julia" : "julia --project=$project_path"
    f1 = Dict(
        "name" => "run_script",
        "path" => RUN_SCRIPT,
    )
    rr_small = Dict(
        "name" => "small",
        "num_cpus" => 1,
        "memory" => "10g",
        "runtime" => "PT30M",
    )
    rr_medium = Dict(
        "name" => "medium",
        "num_cpus" => 8,
        "memory" => "20g",
        "runtime" => "PT2H",
    )
    build_command =
        "$julia_cmd $script setup --simulation-name=$simulation_name " *
        "--num-steps=$num_steps --num-period-steps=$num_period_steps " *
        "--num-overlap-steps=$num_overlap_steps --output-dir=$output_dir"
    work_command = "$julia_cmd $script execute --simulation-name=$simulation_name --index={i} --output-dir=$output_dir"
    join_command = "$julia_cmd $script join --simulation-name=$simulation_name --output-dir=$output_dir"
    num_partitions = get_num_partitions(partitions)
    jobs = [
        Dict(
            "name" => "build",
            "command" => build_command,
            "resource_requirements" => "small",
        ),
        Dict(
            "name" => "job_{i}",
            "command" => work_command,
            "use_parameters" => ["i"],
            "depends_on" => ["build"],
            "resource_requirements" => "medium",
        ),
        Dict(
            "name" => "join",
            "command" => join_command,
            "resource_requirements" => "small",
            "depends_on_regexes" => ["job_\\d+"],
        )
    ]
    workflow = Dict(
        "name" => "RTS_UC-ED",
        "description" => "Partitioned RTS UC-ED simulation",
        "files" => [f1],
        "jobs" => jobs,
        "resource_requirements" => [rr_small, rr_medium],
        "parameters" => Dict("i" => "1:$num_partitions"),
    )

    # TODO: add a post-processing job that loads results.

    filename = "workflow.json"
    open(filename, "w") do io
        JSON.print(io, workflow, 2)
    end
    println("Created Torc workflow in $filename")
end

output_dir = "simulation_output"
mkpath(output_dir)
configure_parallel_simulation(
    RUN_SCRIPT,
    365,
    7,
    output_dir;
    num_overlap_steps = 1,
    project_path = "sienna",
    simulation_name = "rts",
)
 