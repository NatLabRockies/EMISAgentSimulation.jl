# Project Initialization Guide

Project initialization turns a user-provided system input bundle into the directory
tree EMIS needs to run cases. It is meant for a user who has a PSY/Sienna system,
preprocessed time series, outage data, and a small set of CSVs describing zones,
technologies, scenarios, investors, and build options.

The starter input bundle lives at `config/project_templates/project_spec/`. Copy that
folder, edit the CSVs, add your real system data, then run `initialize_emis_project`.

## Basic Workflow

1. Copy `config/project_templates/project_spec/` to a user input folder.
2. Edit `project_spec.csv` with your system name, paths, investors, and generated
   directory names.
3. Edit `zones.csv`, `technologies.csv`, `scenarios.csv`, and `projectoptions.csv`.
4. Add your PSY system JSON, its HDF5 time-series sidecar, outage file, and real
   time series tree.
5. Add optional overrides only when your study needs custom market, finance, or
   case settings.
6. Run `initialize_emis_project(input_dir; output_dir=project_root)`.
7. Run `new_emis_case(project_root, case_name; overrides=...)` for each scenario run.

Keep the user input folder separate from generated output. The input folder should
contain only files the user provides or edits.

## Required User Inputs

These files must be in the copied input folder:

- `project_spec.csv`: paths, investors, and generated directory names
- `zones.csv`: EMIS zones and load-column names
- `technologies.csv`: unit-type classifications and technology defaults
- `scenarios.csv`: scenario names, PCM labels, and weather years
- `projectoptions.csv`: minimal new-build candidates
- `system_config.csv`: runtime reader settings

These data files must exist either in the input folder or at paths referenced from
`project_spec.csv`:

- PSY/Sienna system JSON from `system_filepath`
- matching `<system_stem>_time_series_storage.h5` sidecar
- preprocessed time series tree from `time_series_data_dir`
- outage data from `outage_filepath`, unless the study intentionally uses defaults

Use `config/project_templates/project_spec/timeseries/README.md` for the required
time series folder layout.

## Optional User Inputs

Add these only when needed:

- `projectexisting.csv`: existing-project ownership. Omit it for all-new-entrant
  cases or for investors with no existing projects.
- `devices_to_remove.csv`: devices to prune from the extracted PSY system.
- `<system_stem>_metadata.json` and `<system_stem>_validation_descriptors.json`:
  optional PSY sidecars copied with the system JSON when present.
- `simulation_settings.csv`, `markets_included.csv`, and `options.csv`: case-template
  overrides. If omitted, templates from `scripts/template/` are used.
- `queue_cost_data.csv`, investor finance defaults, and `markets_data/`: study-specific
  market and finance overrides. See
  `config/project_templates/project_spec/markets_data/README.md`.
- `investors/<investor>/markets_data/`: investor-specific market assumptions such as
  `scenario_data.csv`, `scenario_multiplier_data.csv`, and `investor_belief.csv`.
- `project_defaults.csv`: per-`unit_type` economic/technical fallback values (fuel price,
  heat rate, CO2 emission rate, inertia, FOR/MTTR, capex/lifetime, fixed O&M, etc.) used
  to complete `projectexisting.csv` (existing-fleet rows, via PSY-derived values where
  available) and `projectoptions.csv` (new-build candidates) whenever a field isn't
  otherwise supplied. If present in the project input folder, this file *replaces* the
  package's bundled `config/project_defaults.csv` for the whole project — supply it when
  your system's technologies or economics differ from that reference data. It must have
  the same `unit_type,field,value` schema as `config/project_defaults.csv`.
  - The bundled values are drawn from the Annual Technology Baseline: NLR (National
    Laboratory of the Rockies). 2020. "2020 Annual Technology Baseline." Golden, CO:
    National Laboratory of the Rockies. [https://atb.nlr.gov](https://atb.nlr.gov/).
  - A row with `unit_type = ALL` (e.g. `ALL,Online Year,2000`) is a technology-agnostic
    fallback applied to every unit type when no per-`unit_type` row exists for that
    field. Use this for fields like `Online Year` that vary too much within a single
    technology (real fleet vintages span 40+ years per technology) for a per-technology
    average to be meaningful.
  - `Online Year` in particular is genuinely per-generator, not a technology constant.
    Rather than overriding it in `project_defaults.csv`, add an optional `Online Year`
    column directly to your `projectexisting.csv` — any value supplied there for a
    generator takes precedence over both the per-`unit_type` and `ALL` defaults.

`reference_case_dir` is optional. It can supply compatible downstream market, investor,
and finance files, but it does not replace the need for the user's own system, zones,
technologies, scenarios, options, outages, and time series.

## Generated Layout

`initialize_emis_project` writes generated project files under `output_dir`:

- `<base_dir_name>/<heterogeneity>/`: market inputs, investor inputs, and `system_config/`
- `<test_system_dir_name>/`: copied PSY system bundle
- `<test_system_dir_name>/RTS_Data/SourceData/`: derived `gen.csv`, `branch.csv`,
  `dc_branch.csv`, and `reserves.csv`
- `<test_system_dir_name>/RTS_Data/timeseries_data_files/`: copied user time series
- `case_templates/`: rendered run script, settings, market toggles, and options
- `<runs_dir_name>/`: run folders created by `new_emis_case`

`new_emis_case` only stamps a run folder from `case_templates/`. The case data folder
under `<base_dir_name>/<case_name>/` is created later by `CaseDefinition` when the run
script starts.

## Running A Case

After initialization, create a case with `new_emis_case`. Per-case settings can be
changed through overrides, for example `simulation_years=1` for a smoke run. The generated
run script copies the initialized system time series from:

```text
<test_system_dir_name>/RTS_Data/timeseries_data_files/
```

to the run result folder:

```text
<runs_dir_name>/<case_name>/Results/<run_name>/timeseries_data_files/
```

Then `create_agent_simulation` reads the generated base, test-system, market, investor,
and time series inputs.

## Timeseries Naming Contract (Canonical Mode)

A project created from scratch (no pre-built `constructed_systems/` and no legacy raw
weather/load-forecast data under `<time_series_data_dir>/input_processing/`) is built in
**canonical mode**: `create_rts_sys` reads your own time series directly instead of a
legacy system-specific raw-data pipeline. This is the expected, supported path for a
new user's project. Column names must match exactly:

- **Load**: `Load/DAY_AHEAD_regional_Load.csv` and `Load/REAL_TIME_regional_Load.csv`
  zone columns are resolved through `zones.csv` (same as the runtime reader), not by
  literal PSY area name. The load-scaling factor is computed from `sim_year_1`'s peak
  divided by `system_config.csv`'s `default_rts_load` baseline (GW) — set that value to
  your system's intended peak so the scaled profile lines up with your PSY system's
  capacity.
- **Wind/PV**: `WIND/DAY_AHEAD_wind.csv`, `WIND/REAL_TIME_wind.csv`,
  `PV/DAY_AHEAD_pv.csv`, and `PV/REAL_TIME_pv.csv` columns must be named **exactly**
  after the matching PSY generator (case-sensitive), e.g. a PSY generator named
  `Glove Solar` needs a `Glove Solar` column. If a generator's name isn't found, that
  generator is skipped with a `@warn` rather than failing the whole build — check logs
  after a first run to confirm every VRE generator you expect got a profile attached.
- **Reserves**: `reserves.csv` only lists a reserve product when a matching
  `Reserves/DAY_AHEAD_regional_{product}.csv` (or `REAL_TIME_regional_{product}.csv`)
  file exists, matched case-insensitively. Products with no matching file are silently
  dropped from `reserves.csv`, since the runtime reader builds file paths directly from
  that column and errors on a missing file rather than skipping it.

If your project instead supplies legacy raw weather/load-forecast data under
`input_processing/`, an older system-specific construction path is used unchanged for
backward compatibility; this only applies to migrating existing pre-project-init cases,
not to new projects.