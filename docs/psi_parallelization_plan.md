# Parallel (Partitioned) PSI Production Cost Runs — Design & Implementation Plan

Status: **Planned, not implemented.** Branch: `pg/sienna_parallelization`.

Purpose: cut the wall time of the yearly PowerSimulations.jl (PSI) production cost run
inside EMIS. Today one PSI year takes 2–3 days. The plan splits the year into partitions of
fixed length, runs them at the same time on a single node, and joins the results. The
`hpc = false` setup and the single entry point
([run_EMIS_template.jl](../scripts/template/run_EMIS_template.jl)) stay as they are.

This document is meant to be a reusable context file: read it first when picking up this
work in a new session.

---

## 1. Current state

- **Only one place runs PSI:** `create_simulation` in
  [siip_simulation_definition.jl](../src/markets_simulation/siip_simulation_definition.jl#L1511).
  It is called from `energy_mkt_clearing` in
  [actual_energy_mkt_clearing.jl](../src/markets_simulation/actual_energy_mkt_clearing.jl#L93),
  which is called from `create_realized_marketdata` in
  [actual_market_simulation.jl](../src/markets_simulation/actual_market_simulation.jl).
- **Models:** UC (36 h horizon, 24 h interval) and ED (2 h horizon, 1 h interval), plus MD
  (168 h horizon, 72 h interval) when `md_market = true`. The template default is
  `md_market = FALSE`.
- **Steps:** `steps = 365` (1 step = 1 UC interval = 1 day). With MD on, 1 step = 72 h.
- **Sequence:** `PSI.build!(sim)` → `PSI.execute!(sim)` → `PSI.SimulationResults(sim)`, then
  about 650 lines of post-processing that read realized duals and variables
  ([L1848](../src/markets_simulation/siip_simulation_definition.jl#L1848) –
  [L2494](../src/markets_simulation/siip_simulation_definition.jl#L2494)).
- **Solver:** Xpress through `JuMP.optimizer_with_attributes` in the run template.
  `THREADS` is **not** set, so each MIP uses every core. `BARTHREADS = 8`.
- **Workers:**
  - [create_parallel_workers](../src/utils/parallel_utils.jl#L4) adds investor and scenario
    workers: at most 4 locally, and none when investor/scenario parallelism is off.
  - [create_pras_worker](../src/utils/parallel_utils.jl#L67) adds one PRAS worker with
    16 threads and stores it in `PRAS_WORKER[]`.
  - Every `pmap` in `src` (investor predictions, derating, ORDC) uses the **default**
    worker pool.
- **The yearly loop runs strictly one stage at a time**
  ([investment_simulation.jl](../src/investment_simulation.jl)), with no `@async` or
  `@spawn`:
  1. `update_delta_irm!` ([L194](../src/investment_simulation.jl#L194))
  2. investor predictions and iterations
     ([L223](../src/investment_simulation.jl#L223),
     [L244](../src/investment_simulation.jl#L244))
  3. `create_realized_marketdata`, the PSI run
     ([L338](../src/investment_simulation.jl#L338))
  4. RA metrics ([L393](../src/investment_simulation.jl#L393)) and derating
     ([L480](../src/investment_simulation.jl#L480))
- **PSI version:** PowerSimulations 0.37.1, PowerSystems 5.12.3, Julia 1.12.1.
- **Target hardware:** Kestrel bigmem node (104 cores, about 2 TB RAM). The Xpress license
  allows unlimited concurrent instances.

---

## 2. Approach overview

1. **Single entry point.** You run only `run_EMIS_template.jl`, for example:

   `julia --project=. scripts/template/run_EMIS_template.jl --psi-workers 26 --psi-period 14 --psi-overlap 2 --psi-solver-threads 4`

   Without `--psi-workers` (or with 0), the run is sequential exactly as today.
2. **Dedicated PSI worker pool.**
   - Workers are created once at startup, next to the PRAS worker, and live for the whole
     run.
   - Their IDs are stored in `PSI_WORKERS[]`.
   - Partitions run only on this pool.
3. **Each year, inside `create_simulation`:**
   1. Write `sys_MD`, `sys_UC` and `sys_ED` to JSON, after the time-series updates and the
      Reg_Up/Reg_Down service additions.
   2. On the main process, build the full simulation with `PSI.build!(sim; partitions)`.
      This setup step writes `simulation_partitions/config.json` and creates the full-size
      combined results store.
   3. Run `pmap(WorkerPool(PSI_WORKERS[]), i -> run_psi_partition(i, ...), 1:N)`. Each
      worker:
      - loads the systems from JSON;
      - builds the same `Simulation` with
        `simulation_folder = <base>/simulation_partitions/<i>`;
      - calls `build!(sim; partitions, index = i)` and then `execute!`.
   4. Call `PSI.join_simulation(base_dir)`, then `PSI.SimulationResults(base_dir)`.
   5. Pass the joined results to the **unchanged** post-processing code.

---

## 3. How the partitioning works

All of the partitioning is done by PSI. EMIS only picks the partition length:

`PSI.SimulationPartitions(num_steps, period, num_overlap_steps)`

- **Step range for partition `i`** (`get_absolute_step_range`):
  - start = `period × (i − 1) + 1`, moved back by `num_overlap_steps` when `i > 1`;
  - end = `start + period − 1`, or the last step for the final partition.
- **`build!(sim; partitions, index = i)`:**
  - moves the simulation start time forward by `step length × (first(range) − 1)`;
  - sets `steps = length(range)`.
- **`join_simulation`:** skips the first `num_overlap_steps` of each partition after the
  first, so each day appears exactly once in the combined results.

With `md_market = false` (1 step = 1 day):

| `--psi-period` | Partitions | Example ranges (overlap shown) |
|---|---|---|
| 7 (weekly, overlap 1) | 53 | 1–7, 7–14, 14–21, …, 364–365 |
| 14 (biweekly, overlap 2) | 27 | 1–14, 13–28, 27–42, … |
| 30 ("monthly", overlap 2) | 13 | 1–30, 29–60, 59–90, …, 359–365 |

Things to keep in mind:
- Partitions are **fixed-length blocks, not calendar months**.
- With `md_market = true`, 1 step = 3 days, so "monthly" is `--psi-period 10`.
- **Partitions after the first start cold:**
  - PSI's model initialization sets unit status at the new start time.
  - Storage state of charge starts from the system's initial level.
  - The overlap steps act as warm-up and are thrown away at the join.
  - For long-duration storage cases, use an overlap of 3–7 days.

---

## 4. Departure from the PSI documented approach

PSI documents two ways to run a simulation in parallel
([parallel simulations how-to](https://sienna-platform.github.io/PowerSimulations.jl/stable/how_to/parallel_simulations/)):

- **(a) Local:** `run_parallel_simulation(build_simulation, execute_simulation; script, …)`.
- **(b) HPC:** a separate script run as `setup`, `execute --index=i` and `join` commands,
  driven by an external scheduler. This is what JADE, or Torc in
  [configure_parallel_simulation.jl](../scripts/configure_parallel_simulation.jl), does.

This plan uses **neither wrapper as written**. It uses the same public building blocks that
both wrappers call internally.

### 4.1 Why not `run_parallel_simulation` (option a)

In PSI 0.37.1 (`src/simulation/simulation_partitions.jl`), `run_parallel_simulation`:

1. runs the setup build on the main process through `process_simulation_partition_cli_args`;
2. calls `Distributed.addprocs(num_parallel_processes)`;
3. runs `Distributed.@everywhere include($script)` on **every** worker;
4. runs `Distributed.pmap(_run_parallel_simulation, jobs)` on the **default pool (all
   workers)**;
5. in a `finally` block, calls `Distributed.rmprocs(Distributed.workers()...)`, which
   removes **every** worker;
6. runs the join.

It assumes it is the only thing using worker processes in the Julia session. Inside EMIS
that causes these problems:

| `run_parallel_simulation` behaviour | Effect inside EMIS |
|---|---|
| Removes all workers at the end | Kills `PRAS_WORKER[]` and the investor workers. The next `calculate_RA_metrics` fails (`remotecall_fetch` targets a dead worker), and the investor/derating `pmap` calls silently fall back to the main process only. |
| Loads `script` on every worker | The script can't be `run_EMIS_template.jl`, or every worker would start an EMIS run. A second, separate build script is required. |
| `build_function(output_dir, name, partitions, index)` signature | Year-specific systems and settings can't be passed in, so they have to come through global variables, environment variables or files. PSI looks the functions up by name in `Main`; other modules are marked "TODO: not tested". |
| Setup and join go through the CLI handler | Both call `IS.configure_logging(set_global = true, console_level = Warn)`, which replaces EMIS's main-process logger after the first year. |
| `pmap` over the default pool | Partitions also land on the PRAS and investor workers, so the solver thread count per process isn't controlled. |

### 4.2 Why not the setup/execute/join CLI workflow (option b)

- It needs an external workflow tool (JADE or Torc) and several separate `julia` commands
  per year. The requirement is **one script** (`run_EMIS_template.jl`) running the whole
  EMIS loop.
- Each command is a fresh process, so it pays Julia, EMIS and Xpress load and compile time
  every time, and still needs a separate build script.
- The PSI run sits in the middle of EMIS's yearly loop, and the investor and RA stages
  depend on its results. A workflow built outside EMIS would have to be regenerated for
  every year.

### 4.3 What this plan uses instead (public API only)

| Building block | Status in PSI |
|---|---|
| `SimulationPartitions(num_steps, period, num_overlap_steps)` | Exported; used in PSI tests |
| `build!(sim; partitions)` / `build!(sim; partitions, index)` | Documented keyword arguments (how-to "Setup" section) |
| `execute!(sim)` | Public API |
| `PowerSimulations.join_simulation(path)` | Documented in the how-to (called with the module name) |
| `SimulationResults(path)` | Public API |

These are **exactly** the steps `run_parallel_simulation` and the CLI workflow perform.
The only thing that changes is who starts the workers and sends out the jobs:

| | PSI wrapper | This plan |
|---|---|---|
| Who creates workers | `run_parallel_simulation`, on every call | EMIS, once at startup (`create_psi_workers`) |
| How build/execute reach the workers | `@everywhere include(script)` + look-up by name in `Main` | `run_psi_partition` is part of the EMIS module, loaded by the existing `@everywhere using EMISAgentSimulation` |
| Which workers run partitions | Default pool (all workers) | `WorkerPool(PSI_WORKERS[])` |
| Worker cleanup | `rmprocs(workers()...)` | None; workers are reused every year |
| Logger | Replaced globally | Unchanged |

Rules that follow from this:
- **Don't use PSI internals.** No underscore functions, and no reading the partition
  `status.json` files.
- **Failure handling:**
  - `run_psi_partition` raises an error if the build status is not `BUILT` or the run
    status is not `SUCCESSFULLY_FINALIZED`.
  - `pmap` passes the error back, so the join never runs on a failed year.
  - This also avoids a 0.37.1 bug: `join_simulation` → `_check_jobs` checks partition 1's
    status for every index.
- **Differences from the how-to example:**
  - The example passes `serialize = isnothing(index)` to `build!`. That keyword argument
    doesn't exist in 0.37.1, so don't pass it.
  - In 0.37.1 the CLI parser rejects option values that contain `=`. That doesn't matter
    here because the CLI isn't used.
- **Upgrade risk:** about the same as using the wrapper. If PSI changes how `build!` handles
  partitions, the wrapper changes with it.

### 4.4 Fallback (if the wrapper must be used)

`run_parallel_simulation` can be used with these workarounds:
1. Keep a small build script inside the package that reads the year's system JSON paths
   from an environment variable.
2. Remove EMIS's own workers before the call.
3. After the call, recreate the PRAS and investor workers, rerun the `@everywhere` setup,
   and restore the global logger.

This costs a worker restart every year and makes EMIS's worker handling more fragile.
It is not recommended.

---

## 5. Implementation steps

### Phase 0: Measure (no code changes)
1. From an existing run's PSI store, read `PSI.read_optimizer_stats` for UC and ED. This
   gives the per-solve time, the UC vs ED split, and how much slower summer is than the
   shoulder seasons. Use it to pick `--psi-period`.
2. Record the peak memory (`Sys.maxrss()`) of one PSI process to cap the number of workers.

### Phase 1: Refactor `create_simulation` (no behaviour change)
3. Split [create_simulation](../src/markets_simulation/siip_simulation_definition.jl#L1511)
   into three functions:
   - `prepare_psi_systems!(sys_MD, sys_UC, sys_ED)`: the Reg_Up/Reg_Down add-service loop.
   - `build_psi_simulation(sys_MD, sys_UC, sys_ED, solver, …; name, steps, simulation_folder)`:
     templates, problems, feedforwards and sequence. Returns a `PSI.Simulation` that is not
     yet built.
   - `postprocess_psi_results(sim_results, sys_MD, sys_UC, sys_ED, …)`: the current
     [L1848](../src/markets_simulation/siip_simulation_definition.jl#L1848) –
     [L2494](../src/markets_simulation/siip_simulation_definition.jl#L2494) block, changed
     to take a `PSI.SimulationResults`.
4. Check that a sequential run produces the same output as before.

### Phase 2: Worker pool and settings (can run alongside Phase 1)
5. In [definitions.jl](../src/definitions.jl), add:
   - `PSI_WORKERS = Ref{Vector{Int}}(Int[])`
   - `PSI_PARALLEL_CONFIG` (partition length, overlap, solver threads).
6. In [parallel_utils.jl](../src/utils/parallel_utils.jl), add
   `create_psi_workers(hpc, n; node)`, following `create_pras_worker`:
   - HPC: `addprocs([(node, n)])`.
   - Local: `addprocs(n; lazy = false)`.
7. In [run_EMIS_template.jl](../scripts/template/run_EMIS_template.jl):
   - Read `--psi-workers`, `--psi-period`, `--psi-overlap` and `--psi-solver-threads`, the
     same way `--pras-threads` is read.
   - Create the PSI workers **before** the `@everywhere` block.
   - Set `SIIP_TIME_SERIES_DIRECTORY` on the PSI workers.
   - Using command-line flags avoids changing `CaseDefinition` and the checkpoint format.

### Phase 3: Partitioned execution (depends on Phases 1 and 2)
8. New file `src/markets_simulation/psi_partitioned_simulation.jl`, included in
   [EMISAgentSimulation.jl](../src/EMISAgentSimulation.jl), with two functions.

   **`run_psi_partitioned(...)`, on the main process:**
   - Write the three systems to `<results_dir>/psi_partitions/year_<y>/systems/`. This must
     be on a **shared** file system, not node-local `$TMPDIR`.
   - Build with `simulation_folder = <results_dir>/psi_partitions/year_<y>`, then call
     `PSI.build!(sim; partitions)`.
   - Run `pmap(WorkerPool(PSI_WORKERS[]), run_psi_partition, 1:N)`.
   - Call `PSI.join_simulation(base_dir)`, then `PSI.SimulationResults(base_dir)`, then
     `postprocess_psi_results`.
   - Keep setting `current_siip_sim[1]` to the main-process simulation.

   **`run_psi_partition(i, ...)`, on a worker:**
   - Load the systems with `PSY.System(json; runchecks = false)`. `load_sienna_systems!`
     shows this works with EMIS's custom component types.
   - Override Xpress `THREADS` and `BARTHREADS` with the per-worker thread count.
   - Call `build_psi_simulation`, then `build!(sim; partitions, index = i)`, then
     `execute!`, raising an error on any status that isn't a success.
   - Finish with `GC.gc()`.
9. At the top of `create_simulation`: if `PSI_WORKERS[]` is empty, use the sequential path;
   otherwise use `run_psi_partitioned`.

### Phase 4: Validation (depends on Phase 3)
10. Unit/integration test:
    - Test system with HiGHS, about 6 steps, period 2, overlap 1, 2 workers.
    - Compare realized variables and duals between the partitioned and sequential runs, as
      PSI's `test/test_simulation_partitions.jl` does.
    - Skip On/Off/Start/Stop variables, and allow some tolerance on production cost.
11. Compare one real year run both ways on energy and reserve prices, capacity factors by
    technology, start-up/shut-down costs, clean-energy percentage, and total production
    cost.

---

## 6. Expected benefit

These are estimates from the 2–3 day figure, not measurements. Phase 0 will narrow them.

Per-day solve time: $t_{\text{step}} \approx 60\,\text{h} / 365 \approx 10$ min.

$$T \approx T_{\text{json}} + T_{\text{setup build}} + \max_i\big[T_{\text{load+build}} + (p+o)\,t_{\text{step}}\,\kappa\big] + T_{\text{join}} + T_{\text{post}}$$

- $p$ = partition length and $o$ = overlap, in steps.
- $\kappa \approx 1.2$–$2$ is the per-step slowdown from fewer solver threads per process
  and shared memory bandwidth.
- The fixed costs (writing JSON, the one full setup build, the join) add about
  0.5–1.5 h per year.

| Partitioning | Partitions | Threads each | PSI wall time per year | Speedup |
|---|---|---|---|---|
| Monthly (p=30, o=2) | 13 | ~8 | ~8–12 h | ~5–7x |
| Biweekly (p=14, o=2) | 27 | ~4 | ~5–7 h | ~8–12x |
| Weekly (p=7, o=1) | 53 | ~2 | ~4–5 h (best case) | limited by memory, more seams |

- **The year finishes when the slowest partition does.** Summer partitions set the wall
  time, which is why the speedup is well short of the ideal $365/(p+o)$.
- **Weekly partitions aren't worth it.** They add rebuild overhead per partition, 53 copies
  of the systems in memory, and more seams.
- **Realistic expectation:** 4–8x on the PSI stage, so a 2–3 day year would take about
  8–14 h. Because PSI is most of each year's run time, the whole EMIS run should speed up by
  a similar amount (the investor, PRAS and derating stages are unchanged).

---

## 7. Future: stages on separate nodes

The dedicated PSI pool was chosen over reusing the investor and PRAS workers because it
moves to multiple nodes more easily:

- **Nodes:** `create_psi_workers(hpc, n; node)` can put the pool on its own SLURM node
  without touching the partition code.
- **Files:** partition inputs and outputs are files (system JSON and per-partition result
  stores), so partitions on other nodes only need a shared file system.
- **Existing pattern:** it follows the PRAS worker, which is already tied to one role
  (`PRAS_WORKER`).

Still needed before stages can run at the same time (out of scope here):
1. **Explicit pools for the other `pmap` calls.** Give the investor, derating and ORDC
   `pmap` calls their own pools (for example `INVESTOR_WORKERS`). Today they use the
   default pool and would also send jobs to PSI workers.
2. **Dependency analysis between stages.** Within a year, investor decisions come before
   PSI, which comes before RA and derating. The realistic candidates for overlap:
   - independent scenarios within a stage;
   - the delta-IRM PRAS loop;
   - pipelining across years.
3. **More robust worker start-up across several nodes,** such as `SlurmClusterManager`
   instead of `addprocs` over SSH.

---

## 8. Decisions and considerations

- **Out of scope:** multi-node runs, running scenarios or years in parallel, solver
  tuning, changes to `CaseDefinition` or the checkpoint format.
  [configure_parallel_simulation.jl](../scripts/configure_parallel_simulation.jl) (Torc)
  is not used.
- **Solver threads:** always cap Xpress `THREADS` per worker, at about
  `104 ÷ N`.
- **Other `pmap` calls:** they will also send jobs to PSI workers, because they use the
  default pool. That's harmless while stages run one at a time.
- **Disk space:** each partition keeps its own results store, which roughly doubles results
  disk use per year. Consider deleting `simulation_partitions/` after a successful join.
- **Time-series scratch files:** `SIIP_TIME_SERIES_DIRECTORY` can point at node-local
  storage, because each worker rebuilds its systems from the shared JSON files.
