# Project Init Functionality — Design & Implementation Plan

Status: **Phase 1 complete; Phase 2 extraction implementation complete, integration validation remaining; Phase 3 complete; Phase 4 complete; Phase 5 complete; Phase 6 complete; Phase 7 migration integration in progress — reserve-name matching, device-pruning, from-scratch load-scaling, and a `technologies.csv` category-naming mismatch found via end-to-end validation are fixed; full generated-case run verification remains**.
Purpose: make EMISAgentSimulation usable on an arbitrary power system without editing
`src/`, by adding a project-initialization tool that scaffolds every input file and
folder a scenario run needs.

This document is intended to be a reusable context file — read it first when picking up
this work in a new session.

**Scope of this work: a generic project initializer and a case initializer. Nothing else.**
Everything that follows — the hardcode extraction, the PSY extractor, the
constructed-systems builder — exists only insofar as it is required to make those two
entry points produce a runnable project on a new system.

## Updated plan from the dry-run findings

The root-cause fix is not in the downstream simulation logic; it belongs in the project
initialization / data-preparation layer. Integer-like fields such as `Lagtime`, `Lifetime`,
`Capex Years`, and other count/year values must be emitted as canonical integers rather
than float-like strings like `2.0` or `25.0`. The project initializer should therefore
normalize defaults before writing CSVs so the consumer code sees `Int64` values and no
`MethodError` is triggered later when `zeros(length(products), num_profit_years)` is
called.

The full plan is therefore:

1. Preserve the source-of-truth fix in the project-init data-prep layer.
   - `project_defaults.csv` and generated project rows must normalize integer-like values
     before they are emitted.
   - Fractional values such as `FOR` must remain floating-point values.
   - This is a data-contract fix, not a downstream arithmetic patch.

2. Treat the generated project as a reusable system template, not a case directory.
   - `make_case_data_dir` only copies into `<base_dir>/<CASE_NAME>/` when the case dir is
     empty or missing.
   - A stale case directory shadows template edits and must be treated as a common setup
     pitfall in the initializer workflow.
   - The generated system tree should be refreshed cleanly before rerunning any validation.

3. Keep the system filename configurable instead of hardcoded.
   - `SystemConfig` should carry the PSY system filename or equivalent config entry.
   - `create_rts_sys` and any system-construction path should read the configured value
     instead of assuming `DA_sys_zonal_with_storage_capacities.json`.
   - This is the minimal system-level fix required to make a generic project run without
     editing package source.

4. Minimize the default-file footprint to the files that are actually required at runtime.
   - Required defaults should be repository-provided only where they are needed to keep a
     generated project runnable without a reference case.
   - Candidate files: `annual_growth.csv`, `queue_cost_data.csv`, `derating_data/.../derating_dict.csv`,
     investor-level market files (`investor_belief.csv`, `scenario_data.csv`), and any file that
     is truly mandatory for initialization or case creation.
   - Files that are optional or only used in a legacy/ERCOT-specific workflow remain
     reference-case or user-supplied rather than implicit repo requirements.

5. Keep all default precedence rules explicit.
   - Precedence should be: explicit user override > per-investor override > reference-case
     defaults > repo-provided defaults > deterministic fallback/validation error.
   - This avoids generic templates overwriting user-specific investor data by accident.

6. Keep RA tracking in the plan without forcing it into the current implementation.
   - `markets_data/resource_adequacy_targets.csv` is an important runtime pattern and should
     remain tracked as an optional but important feature.
   - The initializer should preserve the current runtime convention (`Metric, Target` rows)
     when a project provides RA targets, but it should not become a mandatory generic input
     for the first pass if the generated project can run without it.
   - This is a plan-only item until a concrete generic default policy is decided.

7. Validate the generic project path end-to-end, not only the source-level CSV writing.
   - Dry-run the generated project through `create_agent_simulation` and
     `run_agent_simulation`.
   - Check the generated raw CSVs, investor directories, market defaults, and PSY file
     wiring before claiming the project-init path is ready.
   - Explicitly verify the generated case template carries any user-supplied
     `outage_filepath` from `project_spec.csv` into the runtime `CaseDefinition`, so the
     outage CSV is not silently dropped or re-inferred downstream.

This is the concrete sequencing for the remaining work: fix the source-layer typing bug,
refresh generated project outputs, make the system filename config-driven, define the
minimal default-file set and precedence order, and keep RA target support tracked as a
forward-looking compatibility item.

Clarification for the generated-case path: the project initializer resolves the
`outage_filepath` from the project spec and renders it into the generated run template as
`OUTAGE_FILEPATH`; the template then passes that value into `CaseDefinition(...,
OUTAGE_FILEPATH, ...)`. The runtime path then reads `get_outage_dir(case)` and passes it
through system creation into the outage attachment step (`add_outages_to_system!` /
`attach_outage_data_from_csv!`). In other words, the outage file is part of the generated
project contract, not a hidden runtime default that is reconstructed later.

## Implementation plan for the remaining generated-project issues

### Issue A: Generated system filename mismatch

Root cause: the generated project scaffolding copies a PSY system into the project tree, but the
runtime consumers still assume a hard-coded default filename such as
`DA_sys_zonal_with_storage_capacities.json`. The generator and runtime therefore disagree about
which system file is authoritative when a non-default system name is used.

Implementation plan:

1. Make the system filename explicit in the project contract.
   - Store the canonical system filename in project metadata and in the generated case template.
   - Treat the value from `system_filepath` / `system_name` as the source of truth, not a runtime
     fallback.

2. Carry the value through the initializer and generated run script.
   - Ensure `initialize_emis_project` resolves the copied system and writes the exact basename to
     the generated template.
   - Ensure the rendered `SYSTEM_FILEPATH` matches the file copied into the project tree.

3. Remove hard-coded runtime assumptions.
   - Replace literal filename lookups in `create_rts_sys` and related save/load helpers with a
     config-backed path selection.
   - Keep the legacy filename as a compatibility fallback only when no generated project config is
     present; do not treat it as the generic behavior.

4. Add validation and regression checks.
   - Fail fast if the generated project indicates a system file that is not actually present.
   - Add a project-init regression test that creates a renamed system file and asserts the
     generated case reads that exact file.

Acceptance criteria:
- a generated project with a renamed system file runs without source edits,
- the template and runtime both refer to the same filename,
- no code path relies on the old hard-coded RTS filename.

### Issue B: Generated projects lack required investor forecast files

Root cause: the generated project scaffolding copies investor directories and some market defaults,
but it does not consistently ensure the runtime-required forecast inputs exist for every investor.
The generated project can therefore look complete while still failing later when investor readers
look for files such as `scenario_data.csv`, `investor_belief.csv`, or the required forecast-related
market bundle.

Implementation plan:

1. Define the required investor market bundle explicitly.
   - List the minimum files each investor must contain at runtime.
   - Separate required forecast assets from optional compatibility data so project init can fail
     early with a clear message.

2. Preserve user-supplied investor market files during generation.
   - Copy every file in `spec_dir/investors/<investor>/` and each investor `markets_data/` bundle
     into the generated investor tree.
   - Apply reference-case inheritance only after spec-provided files are copied so project-specific
     data wins.

3. Add fail-fast validation before case creation.
   - Check each investor directory for required forecast and market files.
   - Raise a clear error naming the investor and missing file(s) instead of letting a deep runtime
     error appear later.

4. Keep precedence rules explicit.
   - Explicit project files > reference-case files > packaged defaults.
   - Defaults are allowed only as a minimal generic fallback, not as a way to hide missing user
     data.

5. Add regression coverage.
   - Verify a valid generated project with all investor forecast files passes generation and case
     creation.
   - Verify a project missing one required investor forecast file fails at initialization with a
     descriptive error.

Acceptance criteria:
- every generated investor directory contains the required forecast and market files,
- project initialization clearly fails if a required file is missing,
- the generated project does not rely on stale or partial investor data.

### Execution order

1. Fix the source-layer typing issue and regenerate clean project outputs.
2. Fix the generated system file naming mismatch.
3. Fix the investor forecast-file generation and validation path.
4. Re-run the dry-run validation through `create_agent_simulation` and `run_agent_simulation`.
5. Re-check the generated case template, runtime file lookup, and investor bundles together before
   locking the generic project-init path.

## Environment Setup & Execution Preamble

Before launching the Julia REPL, running scripts, or executing tests in this environment, execute the following commands in the shell:

```bash
export JULIA_DEPOT_PATH="/projects/gmlcmarkets/GS_EMIS_PACKAGES_PGHOSH/.julia:"
module load julia
module load xpressmp/9.2.5
```

These load the correct Julia version, package depot (containing precompiled dependencies and solvers), and the Xpress solver module required by the simulation stack.

Example test invocations:

```bash
# Run a specific test file
julia --project=. -e 'using EMISAgentSimulation, Test, DataFrames, CSV; include("test/test_project_input_validation.jl")'

# Run package test suite via Pkg
julia --project=. -e 'using Pkg; Pkg.test()'
```

## Current implementation status

Phase 1 provides `SystemConfig` in `src/structs/SystemConfig.jl`. It loads a required
`system_config/` directory from a case data directory or its `Heterogeneous`/
`Homogeneous` template. The tracked ERCOT compatibility bundle is located at
`config/ercot_est/system_config/` and can be copied directly into an existing case
data directory to preserve legacy behavior.

Implemented Phase 1:

- Required configuration files: `system_config.csv`, `zones.csv`, `scenarios.csv`, and
  `technologies.csv`. `devices_to_remove.csv` is optional and produces an empty pruning map
  when absent.
- Zone lookup supports IDs, configured zone names, `area_name` aliases, and load-column
  aliases. ERCOT aliases are data in the tracked `zones.csv`, not source-level mappings.
- Scenario PCM labels, technology classification/MOPR exemptions, configured device
  removals, load scaling, base load year, and load-column offset are configuration-backed.
- Scalar `default_rts_load` and `base_load_year` fall back to `DEFAULT_RTS_LOAD` and
  `DEFAULT_LOAD_YEAR` in `src/definitions.jl` when absent or invalid.
- Active callers were rewired for zone resolution, scenario-to-PCM mapping, project
  technology classes, MOPR gating, representative-period VRE grouping, configurable
  reader column offsets, and configured device pruning.

Validation completed: `test/test_system_config.jl` passes `29/29` with the tracked ERCOT
bundle and an isolated custom two-zone configuration. The wider test entry point reaches
and passes the PSI/PSY compatibility tests (`74/74`), then stops in the pre-existing HDF5
fixture because it calls an unavailable zero-argument `CaseDefinition()` constructor.

The tracked compatibility bundle currently contains exactly:

```text
config/ercot_est/system_config/
├── devices_to_remove.csv
├── scenarios.csv
├── system_config.csv
├── technologies.csv
└── zones.csv
```

Phase 2 input-contract work currently includes:

- `config/psy5/psy_classification_mapping.csv`: user-maintained mapping keyed by
  component type, prime mover, and fuel. It has no PSY-version tags; users update it
  when installed PSY types or enum values change.
- `config/project_defaults.csv`: technology-specific defaults and current-code fallback
  values for fields not available from PSY, including `FOR`, `MTTR Hr`, and fallback
  thermal heat-rate inputs.
- `config/project_templates/project_spec/`: the single copyable input specification.
  Its files use the exact names consumed by the initializer; there is no second
  template naming convention.
- `src/project_init/project_input_validation.jl`: executable validation for mapping
  targets, defaults, investor membership, and option placement requirements.
- `src/project_init/system_extractor.jl`: deterministic PSY classification, zone,
  generator/storage, AC/DC branch, reserve table, and existing-fleet extraction.
  The supported execution paths are `create_agent_simulation`
  for constructing the simulation inputs and `run_agent_simulation` for executing the
  simulation. `reserves.csv` is required for cases created through the current
  `create_agent_simulation` path because its internal readers call `read_test_system`
  and `read_rts` for the RTS test-system path.
- `markets_data/resource_adequacy_targets.csv` is a tracked, optional-but-important
  runtime input for RA accounting. The initializer should preserve the pattern from the
  current runtime path (`Metric, Target` rows loaded into `ra_targets`), but it is
  intentionally a plan item only for now so the generator can keep feature parity without
  forcing every new system to provide a bespoke RA target file.

System-level `rec_eligible` metadata is intentionally not part of this bundle. REC product
eligibility remains controlled by each project's existing `REC Eligible` field; the system
configuration only supplies technology classes and MOPR exemptions.

Phase 1 boundary: the legacy NTP construction path remains intentionally unchanged. Its
ERCOT-specific filenames, generator-name mapping, plant profiles, regulation-service names,
and post-processing are Phase 3 work. Clean-energy accounting continues to use PSY
prime-mover/fuel semantics, while REC-product eligibility remains project-level
(`REC Eligible`).

---

## 1. Problem

Today, running a scenario means executing a script like
`.../HPC_Analysis_Runs_GS/<CASE_NAME>/20260717_run_test.jl`, which requires a large,
undocumented tree of CSVs plus a PowerSystems (PSY/Sienna) system, and the code contains
hard-coded values specific to the RTS-GMLC / ERCOT test systems. Swapping in a new system
currently requires editing package source.

Scope of this plan: **the VRE case only** (`vre_reserves = true`).

---

## 2. Design decisions

| Decision | Choice |
|---|---|
| Init scope | Scaffold + validate. User supplies the PSY system JSON and all timeseries. |
| Config format | CSV only, consistent with existing inputs. No TOML/YAML. |
| Hardcode extraction | Minimal first pass — only what blocks a new system. |
| Defaults source | Repository templates and `config/project_defaults.csv`; an existing reference case is optional supplemental data. Legacy ERCOT compatibility data is tracked as an explicit config bundle. |
| Validation | Lives in the new init module. `src/utils/pre_check_EMIS_run.jl` untouched. |
| Timeseries preprocessing | Entirely the user's responsibility. |
| Build candidates | Minimal `projectoptions.csv` is user-supplied — it is the core scenario design lever; omitted fields are completed from configured defaults and deterministic heuristics. |
| Existing fleet | Derived from the PSY system + outage data + category-keyed defaults; users provide only `Investor` and `GEN_UID` when an existing-project input file is needed. |
| Reserve products | List extracted from PSY `Service` objects; requirement timeseries user-supplied. Optional reserve defaults are compatibility overrides, not required user inputs. |
| DA vs RT | Both supplied by the user. No forecast-error synthesis. |
| Run-script template | Build on `scripts/template/` from PR #38 rather than authoring a competing one. That template is de-personalized but still ERCOT/RTS-specific, so generalizing it is part of this work (§7, Phase 5). |
| Repository placement | In this repo, under `src/project_init/`. Not a separate package — see below. |

### Repository placement

The init tool lives in this repository rather than a new one:

- **The manifest mirrors internal read logic, not a public contract.** Init's job is to
  emit exactly what `gather_data`, `read_test_system`, `create_investors` and
  `create_rts_sys` look for — including undocumented details like the `Min Storgae pu`
  typo, the `SETTING,VALUE,,Comments` header shape, and the `make_case_data_dir` copy
  semantics. Split across repos, every schema change in `src/` silently breaks the
  generator with nothing in CI to catch it.
- **Phase 1 is in-package regardless.** `SystemConfig` and the hardcode extraction are
  edits to `src/`; splitting would mean version-pinning across a repo boundary for what is
  really one change.
- **No dependency argument for splitting.** PowerSystems, DataFrames and CSV are already
  in `Project.toml`, and Phase 3 reuses the forecast-window slicing logic from
  `create_sys_w_updated_ts` — code not stable enough to expose as API.
- **Discoverability.** "How do I run this on my system?" should be answerable from the
  repo the user already cloned.

#### Boundary rule

> `src/project_init/` may depend on the rest of the package.
> Nothing in the rest of the package may depend on `src/project_init/`.

This keeps a later extraction to a `lib/` subpackage or its own repo cheap, should the
lifecycle argument win — a scaffolding CLI does evolve on a different cadence than a
simulation engine. The place this rule is most at risk is `SystemConfig`, which is why it
belongs in `src/structs/` and not in `project_init/`: the simulation code needs it.

The one thing that genuinely does not belong in a source repo is the **reference case
data**. `reference_case_dir` pointing at a filesystem path is acceptable for now; it is
large, ERCOT-specific, and changes for reasons unrelated to code.

---

## 3. Key architectural constraint: system project vs. case run

`make_case_data_dir` in
`src/struct_creators/simulation_structs/agent_simulation_creator.jl` (~L504), called from
the `CaseDefinition` constructor, copies `<base_dir>/Heterogeneous` (or `Homogeneous`) into
`<base_dir>/<CASE_NAME>` at run time — and **skips silently if that directory already
exists** (`if !isdir(case_dir)`). `get_data_dir(case)` is `joinpath(base_dir, name)`.

Consequences:

- `initialize_emis_project` creates a **reusable system project**. It has **no case name**.
- A case name belongs to an individual scenario run, set at run time.
- Init must populate `<base_dir>/Heterogeneous/` and must **not** create
  `<base_dir>/<CASE_NAME>/`; a stale case dir silently shadows every edit to the template.
- `system_config/` must live at `<base_dir>/Heterogeneous/system_config/` so it rides along
  with the copy and is reachable via `get_data_dir(case)` (which is what `gather_data` uses).
- Before a case runs, its `<data_dir>/system_config/` must contain the complete system
  configuration. For legacy ERCOT projects, init emits the ERCOT compatibility bundle into
  the template so `make_case_data_dir` copies it into each case directory.

### Two entry points

```julia
initialize_emis_project(spec_dir; output_dir, reference_case_dir)  # once per system
new_emis_case(project_dir, case_name; overrides...)                # once per scenario
```

---

## 4. Directory ownership

### Created by `init` (system-level, case-agnostic)

```
<project_root>/
├── EMIS_RTS_Analysis/                   ← base_dir
│   ├── queue_cost_data.csv
│   └── Heterogeneous/                    (or Homogeneous/) — the template tree
│       ├── system_config/                system_config.csv, zones.csv, technologies.csv,
│       │                                 scenarios.csv, devices_to_remove.csv
│       ├── markets_data/                 copied from reference case, re-indexed
│       └── investors/<investor>/
│           ├── projectexisting.csv       DERIVED from PSY + outage data + defaults
│           ├── projectoptions.csv        COMPLETED from the user's minimal options input
│           └── characteristics.csv, finance_params.csv, MACRS_Schedule.csv,
│               project_capex.csv, size_dict.csv, markets_data/
├── RTS-GMLC/                            ← test_system_dir
│   ├── <system>.json                     user's PSY system, copied/renamed
│   ├── RTS_Data/
│   │   ├── SourceData/                   DERIVED: gen.csv, branch.csv, dc_branch.csv,
│   │   │                                 reserves.csv
│   │   └── timeseries_data_files/        user's DA/RT drop, arranged
│   └── constructed_systems/              optional; else built on first run
└── case_templates/                       seeded from repo `scripts/template/`
    ├── run_EMIS_template.jl               paths pre-substituted for this project
    ├── simulation_settings.csv
    ├── markets_included.csv
    └── options.csv
```

### Legacy ERCOT compatibility bundle

The package does not retain ERCOT zone, scenario, technology, or pruning tables in source.
Instead, the complete compatibility bundle is tracked at
`config/ercot_est/system_config/`. It may be copied directly into an existing case's
`<data_dir>/system_config/`, or the future initializer may place it under
`<base_dir>/Heterogeneous/system_config/`; `make_case_data_dir` will then copy it into each
new case's `data_dir`.

- `system_config.csv`: reader selection, load scaling, timeseries layout, and column-offset settings.
- `zones.csv`: the eight ERCOT zones and their PSY-area aliases (`FarWest`, `North`, …).
- `scenarios.csv`: `scenario_1`/`scenario_2`/`scenario_3` and their
  `baseline`/`central`/`ira` PCM labels.
- `technologies.csv` and `devices_to_remove.csv`: ERCOT technology classifications and
  pruning definitions.

`load_system_config` requires these files for a runnable case. A missing configuration is a
validation error rather than an implicit ERCOT assumption. Scalar settings may fall back to
the corresponding package constants in `src/definitions.jl`.

Folder names keep the existing RTS convention by default — `EMIS_RTS_Analysis`,
`RTS-GMLC`, `RTS_Data/SourceData`, `Heterogeneous` — so a generated project is
indistinguishable in shape from today's cases and no existing path-handling code has to
change. `project_spec.csv` can override the two top-level names (`base_dir_name`,
`test_system_dir_name`); the inner `RTS_Data/SourceData` layout is fixed because
`rts_reader.jl` and `create_rts_sys` resolve those paths directly.

### Created by the run — init must stay out

| Path | Producer |
|---|---|
| `<base_dir>/<CASE_NAME>/` | `make_case_data_dir` |
| `Results/<run_name>/` | run script `mkpath` / `make_results_dir` |
| `Results/<run_name>/timeseries_data_files/{Availability,Net Load Data,Reserves}/`, `rep_*` | `gather_data`, `read_test_system` |
| `<base_dir>/rep_periods_*.csv`, `cluster_indices_*.csv` | clustering cache |
| `temp_data/` | run script |

---

## 5. User-supplied inputs

### Tier 1 — minimal spec (small hand-written CSVs)

The single copyable, exact-filename starter project is tracked under
`config/project_templates/project_spec/`. A user can copy that directory, edit the
values and paths, and pass it to the initializer without renaming files. Users may
instead generate equivalent files in their own preprocessing workflow; the initializer
cares about the contract, not how the files were produced.

| File | Contents |
|---|---|
| `project_spec.csv` | key/value: `system_name`, `system_filepath`, `scratch_dir`, `outage_filepath`, `heterogeneity`, `investors` (semicolon list), `time_series_data_dir`; optional `reference_case_dir` plus overrides `base_dir_name` (default `EMIS_RTS_Analysis`), `test_system_dir_name` (default `RTS-GMLC`), `runs_dir_name` (default `HPC_Analysis_Runs`) |
| `zones.csv` | `zone_id`, `zone_name`, `load_column`, optional `irm_adder`, `net_cone_adder` |
| `technologies.csv` | `unit_type`, `category`, `class` (thermal/renewable/storage/hydro), `capacity_eligible`, `mopr_exempt`, `macrs_years`, `duration_hr` |
| `scenarios.csv` | `scenario_name`, `probability`, `pcm_label`, `weather_year` |
| `projectoptions.csv` | minimal new-build candidates — see schema in §6 |
| `projectexisting.csv` | optional existing-project ownership input — see schema in §6. Omit for all-new-entrant cases or for investors with no existing projects. |

Per-case levers (`rps_target`, `reserve_penalty`, `pcm_scenario`, market toggles, horizons)
live in the rendered `simulation_settings.csv` / `markets_included.csv` case templates, not
in the system spec. A project spec directory may include `simulation_settings.csv`,
`markets_included.csv`, or `options.csv` to override the default case templates copied from
`scripts/template/`.

### Tier 2 — system and timeseries (must be user-supplied)

- **PSY/Sienna system JSON** plus its required time-series storage sidecar. If
  `system_filepath = my_system.json`, the initializer also requires
  `my_system_time_series_storage.h5` beside it and copies optional PSY sidecars
  `my_system_metadata.json` and `my_system_validation_descriptors.json` when present.
- **Timeseries drop**, already preprocessed, in canonical layout:

```
<ts_root>/{scenario}/sim_year_{n}/
  Load/DAY_AHEAD_regional_Load.csv, REAL_TIME_regional_Load.csv   (Year,Month,Day,Period,<zone cols>)
  WIND/DAY_AHEAD_wind.csv, REAL_TIME_wind.csv                     (Year,Month,Day,Period,<gen names>)
  PV/, RTPV/, Hydro/                                              (same shape, optional)
  Reserves/DAY_AHEAD_regional_{product}.csv, REAL_TIME_regional_{product}.csv
```

- **Outage CSV** (`ThermalFOR_*.csv` style) at a user path.

The starter project provides the `timeseries/` directory as the canonical layout for
profile files. Users are responsible for placing their preprocessed data under the
scenario/year folders and using the documented filenames. The actual profile CSVs
remain user data and are not checked into the repository. `outages.csv` provides the
minimal `GEN_UID`, `FOR`, and `MTTR Hr` shape; users may replace it with the supported
long-form or wide outage format.

The starter project also includes `timeseries/README.md` and a sample
`timeseries/scenario_1/sim_year_1/` tree with the DA/RT load, wind, PV, and reserve
filenames. These are format examples, not a complete dataset. Users may copy the
layout, replace the sample columns and values, or generate an equivalent layout from
an independent preprocessing workflow. Column mappings are supplied directly to the
builder when source names differ from PSY component or service names.

### Repository starter templates

The complete starter project set is:

| Template | User input it scaffolds |
|---|---|
| `project_spec.csv` | System path, reference case, scratch path, outage path, investors, and project directory names |
| `zones.csv` | Zone IDs, names, load columns, and optional market adders |
| `technologies.csv` | PSY-to-EMIS technology classifications and technology defaults |
| `scenarios.csv` | Scenario names, probabilities, PCM labels, and weather years |
| `system_config.csv` | Runtime settings including `timeseries_source` |
| `outages.csv` | Minimal outage/default fields |
| `projectoptions.csv` | New-build candidates |
| `projectexisting.csv` | Existing-fleet ownership assignments |
| `devices_to_remove.csv` | Optional device pruning assignments |
| `timeseries/README.md` | Canonical timeseries layout guide; users replace the sample tree with real scenario data |

The PSY/Sienna system JSON, raw timeseries files, and reference case directory are
intentionally not included as fake data: they are system-specific artifacts supplied by
the user. Their required names and locations are described in the starter README and
the canonical `timeseries_defaults.csv` file.

A populated validation or user input directory contains additional real files because it
is not just the repository starter. In addition to the starter CSVs, it includes the
actual PSY system JSON and HDF5 sidecar, the selected outage CSV, real
`timeseries/{scenario}/sim_year_*` profile files, and any study-specific market, finance,
queue-cost, `simulation_settings.csv`, and `markets_included.csv` overrides.

### Tier 3 — optional overrides (default: repository defaults, optionally copied from a reference case and re-indexed)

`simulation_settings.csv`, `markets_included.csv`, `options.csv`, `annual_growth.csv`,
`energy/reserve_up/reserve_down/capacity/REC` market-parameter files,
`{High|Mid|Low}_reserve_penalty/*.csv`, `derating_data/{scenario}/derating_dict.csv`,
`CarbonTax.csv`, `resource_adequacy_targets.csv`, `queue_cost_data.csv`,
`finance_params.csv`, `MACRS Schedule.csv`, `project_capex.csv`, `sizedict.csv`,
`characteristics.csv`, `symmetric_belief.csv`, `symmetric_scenario_data.csv`.

Init rewrites zone-indexed and scenario-indexed parts of these (`load_zone_{i}`,
`existing_Wind_zone_{i}`, the `zones` column, scenario subfolder names) to match
`zones.csv` and `scenarios.csv`.

---

## 6. Project input schemas and completion

The initializer keeps separate user-facing schemas from the complete downstream EMIS
schemas. Users should provide only the fields below. The initializer creates investor
directories and writes completed `projectoptions.csv` and `projectexisting.csv` files
with every column required by the current project creators.

### `projectoptions.csv`

The required columns are `Investor`, `GEN_UID`, `Unit Type`, and `Size`. `Zone` and
`Bus ID` are optional individually, but at least one must be supplied. If only `Zone`
is supplied, the initializer selects the lowest stable bus ID in that PSY area. If only
`Bus ID` is supplied, it derives the zone from the bus's PSY area. If both are supplied,
they must refer to the same area. Missing placement is an error.

The starter input is [config/project_templates/project_spec/projectoptions.csv](../config/project_templates/project_spec/projectoptions.csv).

### `projectexisting.csv`

The minimal user-facing columns are `Investor` and `GEN_UID`. `GEN_UID` must match an
available PSY generator or storage component. Technical fields are derived from PSY;
conflicting technical overrides are errors. `FOR` and `MTTR Hr` come from the supplied
outage CSV when available, otherwise from [config/project_defaults.csv](../config/project_defaults.csv).
The starter input is [config/project_templates/project_spec/projectexisting.csv](../config/project_templates/project_spec/projectexisting.csv).

Completion precedence is `PSY-derived value > explicit option input > technology
default > deterministic heuristic > validation error`. The `Investor` value must
match an investor listed in `project_spec.csv`.

### Complete downstream schema

The completed files retain the existing schema consumed by the investor and project
creators. In particular, the storage column is spelled `Min Storgae pu` for compatibility.
The complete option schema remains:

Header (exactly as consumed by the investor/project creators; note the `Min Storgae pu`
typo is part of the real schema and must be preserved):

```csv
GEN_UID,Unit Type,Fuel,Category,Size,Min Gen pu,Min Up Time Hr,Min Down Time Hr,Shut Down Cost,Ramp Rate pu/Hr,Input Power Rating pu,Output Power Rating pu,Min Storgae pu,Duration Hr,Round Trip Efficiency pu,Fuel Price $/MMBTU,Start Heat Cold MBTU,Output_pct_0,Output_pct_1,Output_pct_2,Output_pct_3,Output_pct_4,HR_avg_0,HR_incr_1,HR_incr_2,HR_incr_3,HR_incr_4,CO2_Emissions ton/MMBTU,Inertia MJ/MW,FOR,MTTR Hr,Lagtime,Capex Years,Lifetime,Fixed OM Cost per MW,Preference Multiplier,Zone,Bus ID,Capacity Eligible,REC Eligible,Synchronous_Inertia
new_WT_1,WT,WIND,Wind,L,0,0,0,0,1,n/a,n/a,n/a,n/a,n/a,0,0,0,0,0,0,0,0,0,0,0,0,0,3.5,0,0,2,20,30,43000,1,zone_1,101,true,true,false
new_PVe_1,PVe,SOLAR,Solar PV,L,0,0,0,0,1,n/a,n/a,n/a,n/a,n/a,0,0,0,0,0,0,0,0,0,0,0,0,0,2.5,0,0,1,20,30,19000,1,zone_1,102,true,true,false
new_BA_1,BA,N/A,Battery,L,0,0,0,0,1,1,1,0,4,0.85,0,0,0,0,0,0,0,0,0,0,0,0,0,2.5,0,0,1,15,15,2525,1,zone_1,103,true,false,false
new_LDES_1,LDES,N/A,Long Duration Energy Storage,L,0,0,0,0,1,1,1,0,12,0.85,0,0,0,0,0,0,0,0,0,0,0,0,0,2.5,0,0,1,15,15,2525,1,zone_1,104,true,false,false
new_CC_1,CC,NATURAL_GAS,Gas CC,L,0.4,8,8,0,0.6,n/a,n/a,n/a,n/a,n/a,4.5,80,0.4,0.55,0.7,0.85,1.0,7.2,6.9,6.7,6.6,6.8,0.0531,5.0,0.05,24,3,25,30,15000,1,zone_1,105,true,false,true
new_CT_1,CT,NATURAL_GAS,Gas CT,L,0.3,2,2,0,1,n/a,n/a,n/a,n/a,n/a,4.5,30,0.3,0.5,0.7,0.85,1.0,11.5,10.8,10.4,10.2,10.5,0.0531,4.0,0.06,24,2,20,30,11000,1,zone_1,106,true,false,true
```

Column notes:

| Column | Meaning |
|---|---|
| `GEN_UID` | Unique, prefixed `new_`. The `{tech}_zone_{i}` convention is what derating and availability files key off. |
| `Unit Type` | Short code; must appear in `technologies.csv` `unit_type`. |
| `Category` | Join key into `finance_params.csv`, `project_capex.csv`, `MACRS_Schedule.csv` — must match exactly. |
| `Size` | **Not MW.** An `S`/`M`/`L` bucket resolved through `size_dict.csv` keyed by `Unit Type`. |
| Storage-only | `Input/Output Power Rating pu`, `Min Storgae pu`, `Duration Hr`, `Round Trip Efficiency pu`; `n/a` otherwise. |
| Thermal-only | `Output_pct_0..4` + `HR_avg_0`/`HR_incr_1..4` piecewise heat-rate curve; `0` for VRE/storage. |
| `Lagtime` | Construction lead time in years. |
| `Capex Years` | Years over which capital spend is amortized. |
| `Fixed OM Cost per MW` | $/MW-yr. |
| `Zone` | Must match a `zone_name` in `zones.csv`. |
| `Bus ID` | Must exist in the PSY system. |
| `Capacity Eligible` / `REC Eligible` | Baseline flags; MOPR and `battery_cap_mkt` can override at runtime. |
| `Synchronous_Inertia` | `true` only for synchronous machines; drives the inertia-market constraint. |

---

## 7. Implementation phases

### Phase 1 — Externalize blocking hardcodes

**Complete.** `src/structs/SystemConfig.jl` loads the configuration bundle.
`DEFAULT_RTS_LOAD` and `DEFAULT_LOAD_YEAR` remain scalar fallbacks in `src/definitions.jl`.
The ERCOT compatibility bundle replaces source-level RTS/ERCOT lookup tables and must be
copied into legacy case data directories before running them.

Completed active-path rewires:

| File | What |
|---|---|
| `src/test_system_parsers/rts_reader.jl` | Configured zone aliases and `zone_column_start` |
| `src/markets_simulation/ordc_construction/ordc_market_creator.jl` | Configured eligible-zone names |
| `src/utils/siip_psy_utils.jl` | PSY area to configured-zone lookup and load-growth names |
| `src/struct_creators/simulation_structs/rts_psy_creator.jl` | Configured PCM labels, load scalar, and device-removal pruning |
| `src/struct_creators/simulation_structs/project_creator.jl` | Configured technology class for project/device construction |
| `src/struct_creators/simulation_structs/product_creator.jl` | Configured MOPR exemptions and storage classification |
| `src/representative_days/clustering.jl` | Configured technology category for VRE grouping |
| `src/utils/save_load_utils.jl` | Configurable scenario list for legacy data conversion |
| `src/struct_creators/simulation_structs/investor_creator.jl` | Configured default scenario |

The NTP-specific system-construction path and its post-processing are deliberately outside
Phase 1 and remain the responsibility of Phase 3's canonical constructed-systems builder.

### Phase 2 — PSY system extractor

*Parallel with Phase 3; both depend on Phase 1.*
The input-contract foundation, extractor slices, and output orchestration, including
existing-fleet completion, are implemented; the populated system fixture and end-to-end
validation remain to be completed. The current extractor uses concrete PSY subtype dispatch.
Renewable and hydro fuel keys are derived from PSY prime-mover semantics because the
installed PSY API does not expose `get_fuel` for those abstract families.
`src/project_init/system_extractor.jl`:

- `extract_zones(sys)` → `zones.csv` from `PSY.Area` / load zones
- `extract_gen_table(sys)` → `RTS_Data/SourceData/gen.csv` (`GEN UID`, `Unit Type` mapped
  from prime mover/fuel via `technologies.csv`, `Bus`, `Pmax MW`, …)
- `extract_branches(sys)` → `branch.csv`, `dc_branch.csv`
- `extract_reserves(sys)` → `RTS_Data/SourceData/reserves.csv`. This file is required
  for cases passed through the current `create_agent_simulation` and
  `run_agent_simulation` workflow: an internal `read_test_system` call dispatches to
  `read_rts`, which reads it unconditionally. `markets_data/reserve_products.csv`
  remains the market product-selection file, while PSY `Service` objects provide
  service metadata to the constructed-system path. Extraction is concrete-service
  aware: services with `get_requirement` use the PSY value; `ReserveDemandCurve` and
  other services without that field do not use a scalar requirement constraint, so
  `Requirement (MW)` remains missing/blank unless an explicit compatibility default
  is supplied. No error is raised solely because a service is a demand curve. The
  optional compatibility defaults are not part of the minimal user input contract.
- `extract_fleet(sys)` → per-investor `projectexisting.csv`; technical fields from PSY,
  economic/lifecycle fields (FOM, Lifetime, Online Year, Capex Years, Lagtime,
  Capacity/REC Eligible, Preference Multiplier) joined from a category-keyed
  `tech_defaults.csv` seeded from the reference case. `FOR` and `MTTR Hr` use the
  PSY2PRAS-style long-form outage table when available, matching prime mover, fuel,
  and the first capacity limit at or above the device nameplate; a wide per-device
  outage timeseries supplies `FOR` when present, and configured defaults are used only
  when no derivable value exists. Long-form `FOR` values greater than `1.0` are
  interpreted as percentages and divided by `100`; fractional values are retained.
- `write_system_inputs(sys, source_data_dir; ...)` writes the files consumed by the
  current `create_agent_simulation`/`run_agent_simulation` workflow. It writes the
  required `zones.csv`, `gen.csv`, `branch.csv`, `dc_branch.csv`, and `reserves.csv`,
  and optionally routes completed `projectexisting.csv` rows into investor folders.
  The focused phase-2 regression suite validates the empty-system/error paths and
  input contracts; a populated PSY fixture is still required for the round-trip test.
- `reserves.csv` generation only includes a PSY reserve product when a matching
  time-series file is present under `RTS_Data/timeseries_data_files/{scenario}/sim_year_{n}/Reserves/`,
  matched case-insensitively and canonicalized to the on-disk file casing (e.g. `SPIN`
  service name → `Spin` file → `Spin` written to `reserves.csv`). A small,
  explicit `RESERVE_PRODUCT_ALIASES` table in `system_extractor.jl` (`REG_DN` →
  `Reg_Down`, `REG_UP` → `Reg_Up`) bridges known raw-PSY-name-to-file-name mismatches
  that aren't just casing differences, mirroring the same renaming already applied to
  constructed systems in `rts_psy_creator.jl`. Products with no matching file (e.g.
  `NONSPIN` when no reserve time series exists for it) are dropped rather than left in
  place, since `read_rts` reads `reserves.csv` unconditionally and builds file paths
  directly from its `Reserve Product` values.

### Phase 3 — Generic constructed-systems builder

**Current status: complete.** The canonical builder performs the PSY mutation and
serialization path without changing the simulation consumer. Populated-system equivalence
and generated-case checks remain integration validation work for the overall initializer,
not blockers to the builder implementation.

`src/project_init/build_constructed_systems.jl`:

```julia
create_sys_with_timeseries(sys_initial, ts_root, scenario, sim_year, market_stage,
                           horizon, interval, output_file, first_stage, ...)
```

- Reuses the forecast-window slicing/wraparound logic from `create_sys_w_updated_ts`
  (`rts_psy_creator.jl` ~L560–720) and `add_outages_to_system!`.
- `create_sys_with_timeseries` accepts user-owned profile paths instead of fixed ERCOT
  filenames: `load_file`, `wind_file`, `pv_file`, and `reserve_files`. Optional
  `load_columns`, `wind_columns`, `pv_columns`, and `reserve_columns` map source CSV
  columns to PSY component or service names; a single numeric column may be inferred.
- `config/timeseries_defaults.csv` is the visible default path map used when those
  explicit profile paths are omitted. It contains `kind,market_stage,directory,filename`
  rows, with `{product}` supported in reserve filenames. A project-specific file may be
  supplied through `timeseries_defaults_file`; explicit per-call file paths take priority.
- Profiles are read into cyclic forecast windows using the system's forecast count and
  initial timestamp, then attached as PSY `Deterministic` series with the component's
  scaling-factor getter. Reserve profiles are attached to PSY `Service` objects under
  the `requirement` series name. Optional `outages_dir` is passed through the existing
  outage attachment path.
- `generate_canonical_system_bundle` deep-copies a supplied PSY system before creating
  the MD and DA/RT artifacts, so attaching profiles and outages does not repeat or
  mutate the source system across bundle outputs.
- Drops the NTP specifics: `sup3rcc_*` / `preds_*` filenames, `GeneratorNameMapping.csv`,
  the 75 GW load scaler, hardcoded `REG_DN` / `REG_UP` service names. Instead reads the
  canonical CSVs, matches columns to PSY component names directly, and drives service
  names from `markets_data/reserve_products.csv` and PSY `Service` objects. The
  `create_agent_simulation`/`run_agent_simulation` workflow also requires
  `reserves.csv` because the internal `read_rts` call reads it directly.
- Writes `constructed_systems/{scenario}/sim_year_{n}/{MD,DA,RT}_sys_EMIS_*.json` plus
  `MD_num_forecast_*.txt`, using the exact names `create_rts_sys` already expects.
- The old `create_sys_w_updated_ts` stays for existing NTP/ERCOT cases. As implemented,
  `create_rts_sys` auto-detects the mode per project rather than requiring an explicit
  `system_config.csv` key: if `<timeseries_data_dir>/input_processing/` (the ERCOT NTP
  raw-data root) does not exist, `create_sys_w_updated_ts` is called in canonical mode
  instead. In canonical mode it reads the project's own
  `Load/DAY_AHEAD_regional_Load.csv` / `REAL_TIME_regional_Load.csv` (mapped to PSY area
  names via `SystemConfig`) for both the actual load profile and the load-scaling
  reference peak — computed from `sim_year_1`'s canonical peak divided by
  `system_config.csv`'s `default_rts_load` baseline — instead of the hardcoded
  `sup3rcc_ecearth3_*`/`preds_*` ERCOT filenames. This keeps `build_constructed_systems.jl`'s
  `generate_canonical_system_bundle`/`create_sys_with_timeseries` builder available as a
  standalone utility, but `create_rts_sys` was not rewired to call it directly; the
  from-scratch fix lives in `create_sys_w_updated_ts` itself to minimize risk to the
  existing NTP/ERCOT path.

Phase 3 integration checks to carry into the initializer work:

- Add a populated PSY fixture covering load, wind, PV, and at least one reserve service;
  the current regression test validates the filesystem contract and profile-input helpers,
  while the populated fixture belongs with the end-to-end initializer tests.
- The builder validates profile existence, numeric columns, finite values, and component
  column coverage. The caller remains responsible for timestamp ordering and units;
  preprocessing is out of scope and those inputs must be supplied in the documented
  canonical hourly format.
- Wire the completed builder into `initialize_emis_project` once the Phase 4 initializer
  exists.

### Phase 4 — Manifest + initializer

**Current status: complete.** The manifest and top-level initializer entry points are implemented and validated against the workspace layout contract.

- `src/project_init/input_manifest.jl` — declarative table (`build_project_init_manifest` and `validate_project_init_manifest`): path template, scope (system/market/investor/scenario/case), required columns, required flag, and source (`user` / `reference` / `derived`). Single source of truth for generation *and* validation.
- `src/project_init/project_initializer.jl` — `initialize_emis_project(spec_dir; output_dir, reference_case_dir)`:
  - Validates `project_spec.csv` and required key/values (`system_name`, `scratch_dir`, `outage_filepath`, `heterogeneity`, `investors`).
  - Asserts no non-empty `<base_dir>` target or pre-existing case runs exist to avoid silent template shadowing.
  - Creates `<base_dir>/<heterogeneity>/system_config/` using user spec files, fallback `system_config` bundle, or package defaults (`config/ercot_est/system_config/`).
  - Creates `<base_dir>/<heterogeneity>/markets_data/` and per-investor `<base_dir>/<heterogeneity>/investors/<investor>/markets_data/`.
  - Copies the PSY system JSON, required `*_time_series_storage.h5` sidecar, and optional metadata/validation sidecars into `<test_system_dir>/`.
  - Copies user timeseries from `time_series_data_dir` into `<test_system_dir>/RTS_Data/timeseries_data_files/`, preserving the configured `test_system_reader = rts` layout while making the top-level directory names configurable.
  - Copies system-level and investor-level reference assets (`characteristics.csv`, `finance_params.csv`, `MACRS Schedule.csv`, `project_capex.csv`, `sizedict.csv`, `markets_data/`, `queue_cost_data.csv`) when `reference_case_dir` is supplied, and supports top-level or per-investor `spec_dir` overrides.
  - Applies optional spec-local `simulation_settings.csv`, `markets_included.csv`, and `options.csv` as rendered case-template overrides.
  - Emits `<output_dir>/case_templates/` for downstream scenario generation.
- `src/project_init/new_case.jl` — `new_emis_case(project_dir, case_name; overrides...)`:
  - Lightweight entry point that stamps a new scenario directory from `case_templates/` without creating stale target paths in the system template tree.

### Phase 5 — Case templates + CLI

**Current status: complete.** The canonical template and CLI are implemented at
`scripts/template/` and `scripts/init_project/init_project.jl`.

- `run_EMIS_template.jl` uses project-rendered paths, settings, and market toggles;
  creates a timestamped result run by default; copies initialized project timeseries
  into a fresh result run; and restores simulation and Sienna-system checkpoints when
  invoked with `--run_checkpoint <year>` and the original `--run_name`.
- The template constructs `CaseDefinition` from the complete settings schema, passes
  `results_dir = result_path` to `create_agent_simulation`, and configures both
  prediction workers and a dedicated threaded PRAS worker. Worker initialization
  activates the EMIS package project rather than assuming a generated run directory
  contains a Julia project.
- `initialize_emis_project` renders project-owned paths plus default `start_year` and
  `pcm_scenario` values into `case_templates/`. `new_emis_case` stamps case names and
  applies per-case setting overrides.
- The CLI supports `init` and `case`, including both `--override key=value` and
  `--override=key=value` forms.

Focused syntax validation passed for the Phase 5 Julia files, and the CLI override
parser is validated. Full generated-case execution remains Phase 6 integration work.

**Depends on PR #38** (`aa/run_script` → `aa/gs_updates`), which adds
`scripts/template/run_EMIS_template.jl` and `scripts/template/simulation_settings.csv`.
That directory is the canonical location for the run template — this work consumes and
generalizes it rather than authoring a parallel one.

**Important: PR #38's template is a de-personalized ERCOT/RTS run script, not a
system-agnostic one.** It removes the individual user's paths but keeps the ERCOT
directory layout, weather year, scenario naming, and solver. Generalizing it is a
deliverable of this phase, ideally as a follow-up PR onto `aa/run_script` so the two
efforts don't diverge.

`scripts/init_project/init_project.jl` provides the CLI for both entry points. Init copies
the generalized `scripts/template/` into `<project_root>/case_templates/`, substituting the
project's paths; `new_emis_case` then copies that per-project template into each run
directory and substitutes the case name.

#### ERCOT/RTS assumptions baked into PR #38

| Line(s) | Assumption | Fix |
|---|---|---|
| L35–36 | `gs_path` / `user_scratch_dir` are bare undefined identifiers | `{{GS_PATH}}` / `{{SCRATCH_DIR}}` placeholders |
| L45 | `"HPC_Analysis_Runs"` folder layout under `GS_PATH` | `{{RUNS_DIR}}` placeholder, defaulting to `HPC_Analysis_Runs` |
| L56 | `NTP_TIMESERIES_DATA_DIR` hardcoded to the ERCOT sup3rcc dataset | Use the `time_series_data_dir` row already present in `simulation_settings.csv`; delete the literal |
| L60–62 | `"Correlated Outages"` + `ThermalFOR_2011.csv` — ERCOT fleet, 2011 weather year | `{{OUTAGE_FILEPATH}}` from `project_spec.csv` |
| L92–96 | `base_dir` / `test_system_dir` literals `"../../EMIS_RTS_Analysis_GS"`, `"../../RTS-GMLC_GS"`, and a second RTS variant on the non-VRE branch | `{{BASE_DIR}}` / `{{TEST_SYSTEM_DIR}}` placeholders, defaulting to `EMIS_RTS_Analysis` / `RTS-GMLC` (the `_GS` suffix is a per-user fork and should not be the default); non-VRE branch out of scope |
| L100–110 | Copies `EMIS_RTS_Analysis_GS/<CASE_NAME>/timeseries_data_files` into the results dir | Placeholder path; this pre-staged per-case timeseries folder is an ERCOT-workflow artifact and should come from the project tree instead |
| L88 | Reads `markets_included.csv`, which the PR never adds | Add it to `scripts/template/` |
| L190 | `create_agent_simulation(case)` without `results_dir` — already flagged in review | Pass `results_dir = result_path` |

`simulation_settings.csv` in the PR is likewise an ERCOT case file rather than a neutral
default: `name` is a specific ERCOT case string, `pcm_scenario = scenario_1` presumes the
`scenario_1 → baseline` mapping that Phase 1 externalizes, `time_series_data_dir` points at
the NTP dataset, and `start_year` / `total_horizon` / `num_rep_periods` are tuned to that
study. Init should emit it with values drawn from the project spec, not copied verbatim.

Its structure is still the right one to keep: it already carries `name`, `rps_target`,
`reserve_penalty`, `pcm_scenario`, `heterogeneity`, `time_series_data_dir` and the
horizons, which matches this plan's split of per-case levers out of the system spec. Note
the header is `SETTING,VALUE,,Comments` with a blank third column; the validator must
tolerate that.

The Xpress optimizer block (L69–85) stays exactly as it is. Solver choice is not in scope
for this work.

### Phase 6 — Validation + tests

**Current status: complete.** The project-validation entry point and validation regression
are implemented and exercised in the package test suite.

- `src/project_init/validate_project.jl` — `validate_emis_project(dir)`: walks the
  manifest; checks file existence, column headers, and cross-consistency (load-TS zone
  columns vs `zones.csv`; scenario folders vs `scenarios.csv`; option techs ⊆
  `technologies.csv`; scenario probabilities either sum to 1 for a probability-weighted
  set or are all 1 for mutually exclusive case alternatives; PSY generator names vs
  availability-CSV headers).
- `test/test_project_init.jl` validates the generated project tree via the public
  initializer API and the validator itself; it is registered in `test/runtests.jl`.
- `initialize_emis_project`, `new_emis_case`, `validate_emis_project`, and `SystemConfig`
  are exported from `src/EMISAgentSimulation.jl`, and the `project_init/` includes are laid
  out after the simulation core to preserve the boundary rule in §2.
- `validate_thermal_fuel_defaults(technologies, defaults)` in
  `project_input_validation.jl` fails fast at init time if any thermal-class `unit_type`
  in `technologies.csv` has no `Fuel` default in `project_defaults.csv` that
  `PowerSystems.ThermalFuels` can parse — see the Phase 7 `Fuel = "N/A"` defect below.
  It is called from `initialize_emis_project` immediately after `technologies` and
  `project_defaults` are resolved.

**Follow-up gap found in Phase 7 (not yet implemented):** `validate_emis_project` checks
that option technologies are a subset of `technologies.csv`, but nothing validates the
other direction — that every `category` value in `technologies.csv` (and therefore every
category an existing or new project can be assigned) actually has a matching row in each
investor's `finance_params.csv` and `project_capex.csv`. A stale or mistyped category
mapping (e.g. `ST → Steam` when investor finance data only has `Gas ST`) currently survives
validation and only surfaces as a `MethodError` deep inside
`calculate_capcost_and_wacc` (`src/utils/finance_utils.jl`) the first time
`create_agent_simulation` processes an investor that owns a unit of that category. Adding
this cross-check to `validate_emis_project` would catch it at init time with a clear
per-investor, per-category error instead.

### Phase 7 — ERCOT migration integration

**Partially complete.** The tracked ERCOT compatibility bundle exists, the loader requires it,
and the initializer already places the bundled configuration under the generated system tree.

End-to-end validation via an ad hoc from-scratch project-init run (creating a new ERCOT
project with no pre-built `constructed_systems/` and no `reference_case_dir`) surfaced and
fixed the following real defects:

- **Reserve product naming mismatch.** The raw PSY reserve service names (`SPIN`,
  `REG_DN`, `REG_UP`, `NONSPIN`) didn't match the on-disk RTS_Data reserve time-series
  file names (`Spin`, `Reg_Down`, `Reg_Up`, no file for `NONSPIN`) either by case or, for
  `REG_DN`/`Reg_Down`, by spelling. Fixed in `system_extractor.jl` — see the Phase 2
  `reserves.csv` notes above.
- **`devices_to_remove.csv` pruning bugs.** `specify_pruned_units` used
  `AbstractString.(device_names)`, which broadcasts an abstract type as if it were a
  constructor and always throws; fixed to `Vector{AbstractString}(device_names)`.
  `prune_system_devices!` then crashed with `remove_component!(sys, nothing)` when a
  listed device wasn't present in the system; it now skips missing devices with a
  `@warn` instead of erroring, since the pruning list is best-effort per system.
- **Load scaling depended on ERCOT-only raw NTP data.** `create_sys_w_updated_ts`
  unconditionally read hardcoded `sup3rcc_ecearth3_*`/`preds_*` files (under
  `input_processing/`) to compute the load-scaling peak and the load profile itself,
  which don't exist for a from-scratch project. Fixed as described in the Phase 3 notes
  above: `create_rts_sys` now auto-detects this and switches to reading the project's
  own canonical `Load` time series when `input_processing/` is absent.
- **Wind/PV profiles depended on `GeneratorNameMapping.csv`.** The same section also
  read ERCOT NTP plant-profile files and required a name-mapping CSV to bridge PSY
  generator names to differently-named profile columns, which doesn't exist for a
  from-scratch project either. Fixed the same way: when in canonical mode,
  `create_sys_w_updated_ts` reads the project's own `WIND/DAY_AHEAD_wind.csv` /
  `PV/DAY_AHEAD_pv.csv` (or `REAL_TIME_*`) files and indexes columns directly by PSY
  generator name, matching the project-init timeseries contract (`<gen names>` as
  columns) — no name mapping required. If a generator's name isn't a column in the
  canonical file (e.g. it wasn't in the user's supplied profile data), that generator
  is skipped with a `@warn` instead of throwing, so one missing profile doesn't abort
  the whole build; users should check the log for skipped generators after a first run.
- **`queue_cost_data.csv` placed outside the copied template.** `initialize_emis_project`
  wrote it to `base_dir/queue_cost_data.csv`, but `make_case_data_dir` only copies
  `base_dir/<heterogeneity>/` into each case's data dir — so it was never reachable at
  run time (`gather_data` failed with a missing-file error). Fixed to write it to
  `base_dir/<heterogeneity>/queue_cost_data.csv` instead, alongside `markets_data/` and
  `investors/`.
- **`projectexisting.csv` was never completed.** `initialize_emis_project` copied the
  user's minimal `projectexisting.csv` (`Investor`, `GEN_UID` only) verbatim into each
  investor directory instead of calling `extract_fleet` to derive the full downstream
  schema (`Fuel Price $/MMBTU`, heat-rate curve, `FOR`/`MTTR Hr`, etc.), even though
  `write_system_inputs` already supported an `ownership`/`investor_dir` completion path.
  `create_products` then failed at run time with a missing-column error. Fixed by
  reading the spec's `projectexisting.csv` as `ownership` and passing it (with
  `investor_dir`) into the existing `write_system_inputs` call.
- **`config/project_defaults.csv` lacked economic/heat-rate fields entirely.** It only
  covered a subset of unit types and fields (mostly for new-build option completion);
  `extract_fleet`'s existing-fleet completion needs `Fuel Price $/MMBTU`, `HR_avg_0`,
  `HR_incr_1..4`, `Output_pct_0..4`, `CO2_Emissions ton/MMBTU`, and `Inertia MJ/MW` for
  every PSY-classifiable unit type, none of which PSY exposes for this system (confirmed
  by inspecting real generators' operation costs: all use `CostCurve` — `QuadraticCurve`
  or `PiecewiseIncrementalCurve` — never `FuelCurve`, so fuel price/heat rate are not
  separable from the combined $/MWh curve). Fixed by populating `project_defaults.csv`
  for every unit type `classify_psy_component` can produce, using per-technology averages
  computed from the real legacy fleet's own `projectexisting.csv` data (which already
  encodes `HR_avg_0` in Btu/kWh with `Output_pct_0=1`/others `0`, i.e. a single average
  heat rate rather than a multi-segment curve) rather than invented placeholder numbers.
  A project-supplied `project_defaults.csv` in the input folder now overrides the
  package bundle (see `_resolve_project_defaults` in `project_initializer.jl`), threaded
  into both the `projectexisting.csv` and `projectoptions.csv` completion paths.
- **User-supplied `technologies.csv` had a stale `Category` mapping that duplicated an
  unrelated category and never matched any finance data.** An ad hoc integration run
  against a real user project (`Project_init_test/technologies.csv`, plus its already
  materialized `Heterogeneous/` template and `<CASE_NAME>/` case dir) mapped `unit_type
  ST` to `category Steam` and `unit_type GT` to `category Gas CT` (colliding with
  `unit_type CT`'s row). Neither `Steam` nor a `GT`-specific category existed in any
  investor's `finance_params.csv`/`project_capex.csv`, so `calculate_capcost_and_wacc`
  (`src/utils/finance_utils.jl`) hit `findfirst(...) === nothing` and threw a
  `MethodError` deep in `create_investors` for any investor that actually owned a
  `Steam`-labeled existing unit. The repo-tracked bundles
  (`config/ercot_est/system_config/technologies.csv`,
  `config/project_templates/project_spec/technologies.csv`) were already correct
  (`ST → Gas ST`, `GT → Gas GT`); the bug existed only in this project's own spec file
  and its two already-generated copies, so all three had to be corrected by hand to
  `Gas ST` / `Gas GT` to match the investor finance data. This is not a source-code bug,
  but it is a real gap in the current validation coverage: nothing in
  `validate_emis_project` / `src/project_init/project_input_validation.jl` cross-checks
  that every `category` value appearing in a user-supplied `technologies.csv` (or
  produced from it into `projectexisting.csv`/`projectoptions.csv`) actually exists in
  each investor's `finance_params.csv` and `project_capex.csv` `Category` column before
  `create_agent_simulation` is run — see the new item added to Phase 6 below.
- **Completed `projectoptions.csv` rows always wrote `Fuel = "N/A"` for every
  non-renewable technology.** `_complete_projectoptions_for_investor` in
  `project_initializer.jl` derived `Fuel` from a hard-coded ternary
  (`WIND`/`SOLAR`/else `"N/A"`) instead of routing it through the same
  `_coalesce_project_option_value` precedence chain (`input_row` > `reference_row` >
  `project_defaults.csv` > fallback) used for every other field, so it never picked up a
  real fuel for thermal technologies. `"N/A"` is not a member of `PowerSystems.ThermalFuels`,
  so any new-build thermal option that was actually instantiated as a PSY generator hit
  `PSY.ThermalFuels(nothing)` in `create_PSY_generator`
  (`src/struct_creators/simulation_structs/siip_psy_device_creator.jl`). This stayed latent
  through normal option evaluation and only surfaced when the RA/capacity-market backfill
  loop (`create_base_system` in `src/resource_adequacy/ra_utils.jl`) deep-copied a `new_CT`
  template option and materialized it directly via `add_capacity_market_project!`. Fixed by:
  (1) routing `Fuel` through `_coalesce_project_option_value` with fallback `"N/A"` (kept
  for non-thermal classes, which never call `PSY.ThermalFuels`); (2) adding explicit `Fuel`
  rows to `config/project_defaults.csv` for every thermal `unit_type` (`NATURAL_GAS` for the
  gas-fired types, `NUCLEAR` for `NU_ST`); and (3) adding
  `validate_thermal_fuel_defaults` (Phase 6, above) so a thermal `unit_type` missing a valid
  `Fuel` default fails at init time with a named-unit-type error instead of a deep PSY
  `MethodError` during a simulation run. The already-materialized stale `projectoptions.csv`
  files for this project's `CC`/`CT`/`RE_CT` rows were hand-patched from `N/A` to
  `NATURAL_GAS` to match, since a re-run would otherwise be shadowed by the stale
  `<CASE_NAME>`/`Heterogeneous` copy-once behavior noted above.

The remaining migration work is the end-to-end legacy-case check: create a case through
`make_case_data_dir` and verify that the copied configuration reproduces legacy ERCOT
behavior without leaving the generated project in a stale or shadowed state, plus a full
generated-case simulation run (§9 item 7) to confirm the from-scratch canonical
load-scaling path produces sane results.

---

## 8. Out of scope

- The `vre_reserves = false` branch.
- Solver choice and solver attributes — Xpress with the existing attribute block stays.
- Any timeseries preprocessing or weather-year synthesis.
- Building the PSY system itself.
- Medium/maximal hardcode externalization — ORDC discretization and σ factor, PRAS sample
  count and seed, finance defaults (tax rate, debt fraction, discount rates), slack and
  penalty costs, queue-cost size buckets. These stay as constants for now.
- Changes to `src/utils/pre_check_EMIS_run.jl`.

---

## 9. Verification

Ensure the environment preamble in [Environment Setup & Execution Preamble](#environment-setup--execution-preamble) has been run before executing Julia commands.

1. `julia --project=. -e 'using EMISAgentSimulation'` loads clean.
2. `julia --project=. -e 'using Pkg; Pkg.test()'` or running individual test files under `test/` passes.
3. Init the tiny fixture; `validate_emis_project` reports no errors.
4. **Extractor round-trip:** run against `DA_sys_zonal_with_storage_capacities.json` and
  diff generated `gen.csv` / `branch.csv` against the RTS-GMLC originals on key columns;
  compare generated `reserves.csv` as part of the
  `create_agent_simulation`/`run_agent_simulation` compatibility check.
5. **Builder profile attachment:** use a populated PSY fixture and a tiny raw CSV drop to
  verify load, wind, PV, and reserve profiles are attached under the expected PSY series
  names, forecast windows wrap at the input boundary, and outage data is applied when
  supplied.
6. **Builder equivalence:** export canonical timeseries from an existing case, rebuild
  `scenario_1/sim_year_1` DA and RT JSONs, and compare per-component time series against
  the existing `constructed_systems` JSON.
7. Run a generated case for one simulation year and compare year-1 outputs to the existing
   case.

---

## 10. Reference material

- Run template (PR #38, `scripts/template/`):
  <https://github.com/NatLabRockies/EMISAgentSimulation.jl/pull/38>
- Example run script:
  `.../HPC_Analysis_Runs_GS/20260408_High_RECT_Static_ORDC_No_Cap_wo_md_4sdes_12ldes_storff_elcc_High_RPS/20260717_run_test.jl`
- Reference case tree:
  `.../Feb2024_ERCOT_2011_MARKET_Test_PGHOSH/EMIS_RTS_Analysis_GS/Heterogeneous/`
- NTP timeseries → JSON preprocessing (the script Phase 3 generalizes):
  `.../NTP_TimeSeries_Data/input_processing/replace_ts_data_in_json.jl`
- Input layout notes: `input_file_structure.html` at the repo root.
