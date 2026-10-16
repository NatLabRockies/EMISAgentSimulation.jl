# EMIS AgentSimulation Model

The Electricity Markets Investment Suite Agent-based Simulation (EMIS-AS)
model is an agent-based model developed at NLR for simulating annual investment
and retirement decisions of heterogeneous investors in the electricity sector.
EMIS-AS is designed to capture the evolution of the electricity generation portfolio
resulting from the interactions of heterogeneous investors under different policy
and market designs. EMIS-AS not only allows end-users to customize market products
and rules, but also to capture investors' heterogeneous financing parameters,
technology preferences, beliefs about the future (forecasts), ability to update
those forecasts, and risk preferences under uncertainty.

## Model Structure

<img src="EMIS-AS_structure.png" />

EMIS-AS is initialized based on the input data detailing the test system, generation
units, market design, investors' characteristics and simulation parameters. Additionally,
the pre-processing step includes calculation of the weighted average cost of capital 
(WACC) and adjusted CAPEX for all existing and option projects for each investor.
Next, the model selects the specified number of representative days (with hourly 
resolution) to be used by the investors for price prediction. These representative 
days and their corresponding weights are determined based on load and renewable 
generation timeseries using k-medoids clustering.

Subsequently, the simulation commences with successive yearly iterations for the specified
simulation horizon. At the start of each simulation year, the investors project future
prices and make revenue predictions. The predicted revenues are then used to calculate the
expected utility (based on the expected net present value (NPV)) for all projects.
Subsequently, the revenue prediction outcomes and expected utility are used to make
investment and  retirement decisions and for calculation of market bids.


Once all investors have completed their decision-making process, their investment and 
retirement decisions are announced. The model then updates the build-phases of all
projects, which are depicted below.

<img src="project_phases.png" />

The projects in the Option phase which get decided to be invested in are sent to the 
interconnection queue and included in the list of active projects. The projects already
in the interconnection queue progress one year further in the queue.
The projects which have completed their queue time then enter the construction phase. It
is assumed that the investors only have to pay the annual queue costs when the projects are
in the queue, but would have to pay the full capital cost once construction commences.
The projects which have completed their construction times are included in the set of existing
projects. Finally, if any project is to be retired, either due to end-of-life or profitability
reasons, is removed from the list of active projects.

The life cycle phase updates are followed by the actual clearing of the modeled electricity 
markets. The current version of EMIS-AS includes energy, ancillary services, capacity and
Renewable Energy Certificates (REC) markets. The investors then update the realized revenues
for their projects and also update their beliefs (using Kalman Filters) based on the realized
values of the parameters. Finally, the derating factors of variable renewable generation 
are calculated based on the updated installed capacities using the top-n net-load hour methodology.

## Loading Data and Simulation Creation

While the model's input data for investors and markets can be instantiated directly as
Julia structs, the model provides a CSV-based file import functionality that may be 
more convenient. Data organized according to the file structure 
[described here](input_file_structure.html) will be automatically populated into a
corresponding `AgentSimulation`. The worklow for processing the input data for creating
the `AgentSimulation` is depicted below.

<img src="simulation_creation.png" />

## Initializing a User-Provided Project

Users can bring their own power system and datasets to initialize an EMIS project. See
the [Project Initialization Guide](src/project_init/README.md) for the required inputs
and setup steps. A starter template is available in
`config/project_templates/project_spec/`.

## Getting the Example Data (Git LFS)

The CSV files under `config/example_systems/` (about 63 MB) are stored with
[Git LFS](https://git-lfs.com). The unit tests and the integration test read them. Without
Git LFS, a clone contains small text pointer files in their place, and those tests fail.

1. Install Git LFS once per machine, using your package manager (for example
   `sudo apt install git-lfs` or `brew install git-lfs`), a download from
   [git-lfs.com](https://git-lfs.com), or a module on HPC systems that provide one. Then enable it
   for your user account:

   ```bash
   git lfs install
   ```

2. Clone the repository as usual. With Git LFS enabled, the CSV files are downloaded during the clone.
   If you cloned before installing Git LFS, download them from the repository root:

   ```bash
   git lfs pull
   ```

3. Check that the files were downloaded. `git lfs ls-files` marks downloaded files with `*` and
   pointer-only files with `-`:

   ```bash
   git lfs ls-files | head
   ```

   A pointer file starts with `version https://git-lfs.github.com/spec/v1` instead of CSV data.

To download only part of the data, use `git lfs pull --include="config/example_systems/rts_gmlc/**"`.

## Running the Integration Test

The unit tests (`julia --project=. -e 'using Pkg; Pkg.test()'`) are quick. The integration
test runs a 2-year, 3-zone RTS-GMLC case end to end and checks the results. It needs a
solver (Xpress) and the example data from Git LFS (see above), and takes 1-2 hours. Run it as a
batch job, or directly in the background (step 2). Everything it uses is in
[test/integration_test](test/integration_test):

- `integration_test_rts_gmlc.sh`: the batch script that runs the whole test.
- `setup_case.jl`: initializes the example spec, validates it and creates the case.
- `check_run.jl`: runs the result checks; it can also be used on its own (see below).

1. From the repository root, open [test/integration_test/integration_test_rts_gmlc.sh](test/integration_test/integration_test_rts_gmlc.sh)
   and add your environment setup in the marked block (for example module loads, `JULIA_DEPOT_PATH`
   and solver license variables).
2. Run the script, either through a scheduler or directly.

   **a. With an HPC scheduler.** With SLURM, from the repository root:

   ```bash
   sbatch --job-name=rts_gmlc_2yr --account=<account> --partition=<partition> --nodes=1 \
          --time=04:00:00 --output=rts_gmlc_2yr_%j.out test/integration_test/integration_test_rts_gmlc.sh
   ```

   **b. Without a scheduler** (e.g., on a personal computer). The
   script runs directly with `bash`. Outside SLURM, the repository root defaults to the folder above
   `test/`, and the run name gets a timestamp in place of the job ID. Set the scratch directory:

   ```bash
   export EMIS_SCRATCH_DIR=$HOME/emis_scratch      # any local folder with several GB free
   mkdir -p "$EMIS_SCRATCH_DIR"
   export RUN_ARGS="--pras-threads 4"              # optional: PRAS threads, default 16; match your cores
   nohup bash test/integration_test/integration_test_rts_gmlc.sh > rts_gmlc_2yr.out 2>&1 &
   tail -f rts_gmlc_2yr.out
   ```

   `nohup ... &` keeps the 1-2 hour run going after you log out. Leave it out to run in the
   foreground. Do not pass `--hpc` here: it looks up the node list from SLURM
   (`SLURM_NODELIST`) and fails outside a SLURM job. The machine also needs Julia and a licensed
   Xpress installation, set up the same way as in step 1.

   For both options, settings can be changed by exporting variables before running the script;
   they are documented in the script. The most useful are `EMIS_SCRATCH_DIR` (default
   `/scratch/$USER`), `EMIS_RTS_OUTPUT_ROOT` (default: a folder under the scratch directory; needs
   several GB), `CASE_NAME`, `RUN_NAME` and `RUN_ARGS` (extra arguments for the run script, such as
   `--hpc` under SLURM or `--pras-threads <n>`).
3. The script initializes the committed spec in `config/example_systems/rts_gmlc/project_spec`,
   validates it, creates a case, runs 2 simulated years and then runs the result checks. The job
   exits nonzero if a hard check fails. The check report is printed at the end of the job output,
   and the results are in `<output root>/project/HPC_Analysis_Runs/<case name>/Results/<run name>`.

To check any finished run, including runs from other cases, without re-running it:

```bash
julia --project=. test/integration_test/check_run.jl --results-dir <path to Results/<run name>>
```

The checks cover the expected result files, load peaks against the time series, zone-average energy
price distributions, lost load, resource adequacy metrics (LOLE, NEUE), and installed, new and retired
capacity by technology. Hard failures (missing files, load peak mismatch, NaN or negative prices or
RA metrics, no installed capacity) fail the run; the other checks print warnings. The full report is
written to `run_checks.csv` and `run_summary.csv` in the results directory. Options such as
`--price-cap`, `--max-lost-load-share` and `--min-capacity-margin` are listed at the top of
[test/integration_test/check_run.jl](test/integration_test/check_run.jl).
