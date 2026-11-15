---
name: emis
description: Verified facts for working with EMISAgentSimulation.jl — project initialization (initialize_emis_project, project_spec, write_system_inputs), what scenarios.csv and investor files actually control, which code uses unit sizes and which cancels, the gen.csv PMax fix, and how to run tests, jobs and plotting on Kestrel. Use before building or changing an EMIS project/spec/case, debugging investor-side data, or running EMIS tests and jobs.
---

# EMIS — verified knowledge

Every entry was checked against code, data or a run. Format: **claim** — evidence — date. Add new findings here in the
same turn they are found; mark unconfirmed items *hypothesis*; correct wrong entries explicitly. PSY/PSI details are in
the `sienna-psy` skill, PRAS/derating in `sienna-pras`.

Code paths are relative to `src/`, branch `aa/Claude` @ 2fe9c10 (+ uncommitted `system_extractor.jl` fix 2026-11-15);
re-grep line numbers.

## 1. Project initialization (`project_init/project_initializer.jl`, `initialize_emis_project`)

- **Existing-unit `Size` and `gen.csv` "PMax MW" are derived from the PSY system** (`write_system_inputs` →
  `extract_fleet`, `extract_gen_table` in `project_init/system_extractor.jl`). From the spec's `projectexisting.csv` only
  `Investor`, `GEN_UID` and an optional `Online Year` are used (`_ownership_override`); all other existing-unit economics
  (heat rate, fuel price, fixed O&M, capex, lifetime) come from `config/project_defaults.csv` (ATB) unless the spec
  supplies its own `project_defaults.csv`. — 2026-11-14
- **A complete `projectoptions.csv`** (all `PROJECT_OPTION_DOWNSTREAM_COLUMNS`) is kept verbatim, filtered by an
  `Investor` column (`_complete_projectoptions_for_investor`); a minimal one is completed from defaults. — 2026-11-14
- Spec top-level `characteristics.csv`, `finance_params.csv`, `MACRS Schedule.csv`, `project_capex.csv`, `sizedict.csv`
  are copied into **every** investor, overriding per-investor files; keep per-investor files under
  `spec/investors/<investor>/` when they differ. — 2026-11-14
- `system_config/` bundle copies only `system_config.csv, zones.csv, technologies.csv, scenarios.csv,
  devices_to_remove.csv` (`_write_system_config_bundle`); `unit_type_aliases.csv` is not copied. — 2026-11-14
- **`config/ercot_est/system_config/devices_to_remove.csv` lists 62 RTS-GMLC unit names** (e.g. `115_STEAM_1`), none in
  the ERCOT system; they are only skipped with warnings (`prune_system_devices!`). — 2026-11-14
- An owned unit that is `available = false` in PSY makes `extract_fleet` error; remove it from ownership. — 2026-11-15

## 2. What scenarios.csv and investor files control

- **The runtime scenario list comes from investors' `markets_data/scenario_data.csv`** (`get_all_scenario_names`,
  `utils/market_utils.jl:995`), not from `scenarios.csv`. ED and PRAS systems are built per scenario; MD/UC once, from
  the `pcm_scenario` setting. — 2026-11-14
- In `scenarios.csv` only `scenario_name` matters (validation: every `timeseries/<scenario>/` folder must be listed;
  derating seed per listed scenario; `get_default_scenario` = first row). `probability` and `weather_year` are parsed but
  never used; investor weights come from `scenario_data.csv` `probability`. `pcm_label` is only used to build file names
  in the legacy (NTP `input_processing/`) path. — 2026-11-14
- The validator accepts scenario probabilities that sum to 1 **or** are all 1.0. — `project_init/validate_project.jl:95-115`

## 3. Code that uses unit sizes (`get_maxcap` / `Size`)

Cancels (profile ÷ size × same size): Sienna dispatch, PRAS capacity, investor energy (`operating_profit.jl:268-282`,
`buildphase_conversion.jl:123,135`), realized capacity factors and reserve % (`siip_simulation_definition.jl:382-568`,
÷ `get_device_size` = `get_rating` for RenewableDispatch, `utils/general.jl:60`), realized energy/REC revenue.

Does **not** cancel (a wrong size changes results): fixed O&M = `$/MW × size` (`project_creator.jl:418`) → NPV and
retirement (`npv_functions.jl:24`); TopNetLoad/ELCC derating denominators; capacity-market supply quantity
(`actual_capacity_mkt_clearing.jl:33`) and revenue (`utils/market_utils.jl:330`, `total_profit.jl:65,153`); investor CEM
existing capacity (`market_project_creator.jl:299`); non-ORDC reserve scaling by PV+wind capacity growth
(`actual_market_simulation.jl:386`). — 2026-11-14

## 4. gen.csv PMax bug (fixed 2026-11-15, uncommitted)

- **Before the fix, project-init `gen.csv` PMax for wind/PV was 100× too small:** `_component_capacity_mw` fell back to
  `get_rating` (system per unit) for RenewableDispatch. The investor reader computes availability = profile ÷ PMax
  (`test_system_parsers/rts_reader.jl:246-283`), so wind/PV availability was ~100× (RTS run 19834716: 303_WIND_1 max 99.2)
  and enters the investor CEM unclipped (`struct_creators/market_structs/market_project_creator.jl:10-33`). Affects every
  project built by project init before the fix (all Nov 8–11 RTS ORDC/seasonal runs). GS legacy (hand-made `gen.csv`) is
  not affected.
- Fix: `_component_capacity_mw` returns `get_max_active_power × system base power` for `RenewableGen`
  (`system_extractor.jl:109-125`). Regression test: last testset in `test/test_project_input_validation.jl` (fails with
  PMax 0.6 vs 60 without the fix). Full suite job 19898822: 1414 pass, 4 broken (pre-existing). — 2026-11-15
- Check after any project init: `gen.csv` PMax must equal `projectexisting` Size for every owned unit.

## 5. Running on Kestrel

- **Tests:** `test/includes.jl` loads Xpress, which needs
  `module use /nopt/nrel/apps/software/xpressmp/modules/default; module load xpressmp/9.2.5` and
  `LD_LIBRARY_PATH=/projects/gridra/aayad/julia/artifacts/6eee2bac22f7dfdc10fb702aa4772f3528a48cb0/lib:$LD_LIBRARY_PATH`;
  without them every test errors during `Xpress` initialization. One file:
  `julia --project=test test/runtests.jl test_project_input_validation`. The 4 `@test_broken` in `test/test_ordc.jl`
  (lines ~241, 310, 312, 466) are known problems, not regressions. — 2026-11-15
- Julia: `/home/aayad/.juliaup/bin/julia` (use the absolute path in batch scripts). — 2026-11-14
- **Plotting:** system `python3` has numpy/pandas only via user site-packages (`python3 -I` hides them) and a broken
  matplotlib; use `/nopt/nlr/apps/kestrel-cpu/anaconda3/2025.12-2/bin/python`. — 2026-11-14
- `ls` is aliased to `ls -l`; `ls -d $VAR` in scripts produces extra fields. — 2026-11-14
- **`sacct` shows `COMPLETED 0:0` even when Julia fails** inside a torc/batch job; always read the job's log. — 2026-11
- Short jobs: partition `short` (4 h limit), account `gmlcmarkets`. — 2026-11-14
