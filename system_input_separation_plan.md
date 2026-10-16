# System Input Separation & Public Release Plan

Status: **plan, with every finding below verified against code, data, or a test run
on 2026-09-30.** Nothing described in §4 has been implemented yet.
Branch: `aa/set_system_inputs` @ `16704d9` = `origin/pg/create-project-init`
(`cd2ba58`, [PR #51](https://github.com/NatLabRockies/EMISAgentSimulation.jl/pull/51))
+ one commit ignoring `test/test_systems/`.
Related in-progress branch: `origin/pg/rts-gmlc-test-system` @ `3c32e81` (Purboday,
not final). It is one commit on the same base (`cd2ba58`) and merges cleanly
into this branch. Findings in §3.7; steps in §4 adjusted to build on it.
Supersedes the 2026-09-18 draft of this file.

## 1. Goal

Two goals that overlap but are not the same:

1. **Portability.** EMIS runs against any user-supplied Sienna/PSY system. Every
   system-specific fact (zones, technologies, scenarios, outage defaults, calendar)
   is a project input read through `SystemConfig` / `input_manifest.jl`, never a
   code-level assumption. ERCOT/RTS survives only as a bundled default.
2. **Public release without data.** The code can be published with no proprietary
   or project data in the tree *or in git history*, and a new user can bring
   their own data in a documented, validated format.

Rule for every step: a test must derive its expected answer from real data or
from an independent source, not restate what the code or config already assumes.
§3.1 shows why: the current test pins a mapping that is 0/8 correct.

## 2. Baseline (verified 2026-09-30)

- Project-init test files (`test_system_config`, `test_project_input_validation`,
  `test_project_init`, `test_constructed_systems_generator`): **96/96 pass** on
  `16704d9`. They pass *including* the wrong zone mapping (§3.1).
- **The package does not precompile.** Julia 1.12 raises
  `Method overwriting is not permitted during Module precompilation` because
  `src/resource_adequacy/parsers/power_system_table_data.jl:120` redefines
  `PSY.PowerSystemTableData` with PowerSystems' exact signature. Julia falls back
  to loading from source, so every run pays full compile time. The compile cache
  dir holds only 0-byte failed temp files. Pre-existing since 2022, also on
  `aa/cap_markets`.
- **The GS_AAYAD HPC environment is stale for this branch.** Its `Manifest.toml`
  lists EMIS without `JSON`, which PR #51 added (`bf6a20e`). Loading EMIS from that
  env fails with `EMISAgentSimulation does not have JSON in its dependencies`.
  That env dev-links **this working copy**
  (`path = /projects/gmlcmarkets/GS_EMIS_packages_AAYAD/EMISAgentSimulation.jl/`),
  so any branch checkout here changes what GS_AAYAD production jobs run.
- Tests were run in an isolated copy of that env, resolved in a scratch dir.
  The production `Manifest.toml` was verified unchanged.

## 3. Verified findings

### 3.1 Legacy zone mapping is wrong, 0/8 (correctness bug, not portability)

Ground truth from `test/test_systems/sys_UC_year1.json` (the GS system, 1,125
`PowerLoad`s) vs. GS_AAYAD
`RTS-GMLC_GS/RTS_Data/timeseries_data_files/scenario_1/sim_year_1/Load/DAY_AHEAD_regional_Load.csv`.
Two independent methods agree:

- **Hourly profile correlation.** Each area's summed PSY load forecast (first 60
  days) correlates 1.000 with exactly one CSV column.
- **Magnitude.** Area share of total `max_active_power` matches column share of
  total peak to within 0.007 for all 8.

| CSV column | True area (derived) | `config/ercot_est/.../zones.csv` says | Positional fallback (`zone_$(index)`) says |
|---|---|---|---|
| 1 | NorthCentral | FarWest ✗ | NorthCentral ✓ |
| 2 | East | North ✗ | East ✓ |
| 3 | Southern | West ✗ | Southern ✓ |
| 4 | Coast | Southern ✗ | Coast ✓ |
| 5 | West | NorthCentral ✗ | West ✓ |
| 6 | North | SouthCentral ✗ | SouthCentral ✗ |
| 7 | SouthCentral | Coast ✗ | North ✗ |
| 8 | FarWest | East ✗ | FarWest ✓ |

- `zones.csv` is a verbatim copy of the old hardcoded `Dict` in `find_zonal_area`
  (introduced in `442f629`, moved to config in `c9c5f2d`). It was never derived
  from data. **Production behavior was already wrong before PR #51.** PR #51
  preserved it.
- `test/test_system_config.jl:64-75` pins the wrong mapping.
- The positional fallback in `find_zonal_area`
  (`src/utils/siip_psy_utils.jl:589-601`) follows `PSY.get_components(Area)`
  iteration order, which follows JSON order. It is 6/8 right by coincidence and
  swaps North/SouthCentral. It is not a safe fallback.
- **Where it bites.** GS `annual_growth.csv` is not uniform: `load_zone_3` and
  `load_zone_6` grow at 0.63%/yr vs. 1.34% for the others (2018). The lookup
  `area → get_zone_name → "load_zone_N"` is used in
  `apply_PSY_past_load_growth!` (`siip_psy_utils.jl:936`),
  `ra_utils.jl:267`, and `actual_market_simulation.jl:43`. With the current
  mapping, the low growth rate goes to West + SouthCentral (~15.7 GW) instead of
  Southern + North (~8.7 GW).
  *Assumption to confirm:* that `annual_growth.csv` column N was meant to follow
  load-CSV column N.
- `eligible_zones` is `1;…;8` in every GS market file, so market eligibility is
  unaffected.

### 3.2 `src/project_init/` validation gaps (verified by test)

- **`_base_project_paths` looks for timeseries in the wrong place.**
  (`validate_project.jl:21-35`) It returns `<project>/timeseries`, ignoring
  `timeseries_data_dir` in `project_metadata.csv`. `initialize_emis_project`
  writes timeseries to `<project>/RTS-GMLC/RTS_Data/timeseries_data_files`. So
  `_validate_load_zone_columns` and `_validate_psy_generator_names` both exit
  early (`isdir(ts_root) || return nothing`) for **every** initialized project,
  and the existing tests never exercise them.
- **`_validate_psy_generator_names` does nothing** (`validate_project.jl:206-234`).
  Reproduced: after adding a `THIS_GENERATOR_DOES_NOT_EXIST` column to a WIND
  csv, `validate_emis_project` still passes.
- **Hardcoded thermal list.** `thermal_types = Set(["ST","CT","CC","GT","RE_CT","IC","NU_ST"])`
  (`project_input_validation.jl:153`, moved from :125 by `cd2ba58`) duplicates
  `technologies.csv`'s `class` column. `cd2ba58`'s new
  `validate_thermal_fuel_defaults` already uses `class == "thermal"`; follow
  that pattern.
- **Hardcoded folder names.** The initializer uses `"EMIS_RTS_Analysis"`,
  `"RTS-GMLC"`, and `"RTS_Data"` as layout names (`project_initializer.jl`, 4
  occurrences; also `agent_simulation_creator.jl` ×2 and `project_creator.jl` ×2).
  Cosmetic, but it leaks the test-system name into every user's project.

### 3.3 `src/resource_adequacy/` (corrected from the 09-18 draft)

What is actually live:

| Item | Status (verified) |
|---|---|
| `power_system_table_data.jl` | Redefines 7 PowerSystems functions (`PSY.PowerSystemTableData` ×2, `PSY.System`, `PSY.make_generator`, `make_thermal_generator(_multistart)`, `make_renewable_generator`, `make_storage`). **EMIS never calls any of them.** They exist only to override PSY, and they break precompilation (§2). The only live function is `add_outage_info!` (+ `transform_outage_to_rate`), called from `siip_psy_device_creator.jl:70`. |
| `parsers/descriptors/power_system_inputs.json`, `generator_mapping.yaml` | Used only by the dead override above. `power_system_inputs.json` is an older fork of PSY's own descriptor (107 lines differ from PSY 5.11.1). |
| `PSY2PRAS.jl:20-23` `OUTAGE_INFO_FILE` → `df_outage`, `outage_values` | Read at **import time on every run**, but only *consumed* by `make_pras_system` → `construct_smc_unavailabilities`. That path runs only when `ordc_unavailability_method == "SMC"`. All GS_AAYAD cases use `Convolution`. |
| Derating / capacity accreditation | Already migrated to `make_pras_system_spi` (`derating_factor_calculator.jl:484,525,672,763`) and `SPI.generate_pras_system` (`ra_utils.jl:70`). |
| `ra_utils.jl:61` `"ThermalFOR_scenario_1_new.csv"` | **Commented out.** Not live. The 09-18 draft was wrong here. |

### 3.4 Calendar (corrected: the 09-18 draft proposed the wrong fix)

- `SIM_START_DATE = 2018-01-01` / `SIM_END_DATE` (`definitions.jl:29-30`) is used
  in `rts_psy_creator.jl` (4×), `siip_psy_utils.jl` (6×), and `general.jl:380`.
- The data contract is **8760 rows per simulation year, no Feb 29**. Verified:
  GS DA and RT load CSVs have 8760 rows, 0 Feb-29 rows, periods 1-24.
- `season_hour_columns` (`general.jl:375-389`) *deliberately* buckets months on a
  non-leap reference year so it lines up with 8760-hour blocks. Tested: with a
  2020 (leap) reference, **240 hours of 8760 fall in the wrong month** (hours
  1417-8040, one day at each boundary from March to December), and December
  loses 24 hours.
- Therefore, making `SIM_START_DATE` configurable **would create** the
  mis-bucketing bug if a user picked a leap year. The fix is to separate two
  concepts (step 5).

### 3.5 Public-release blockers (missing from the 09-18 draft)

- **Git history contains data.** The pack is 36 MB, while data files in the
  current tree total ~75 KB. Removed-but-in-history:
  - `data/RTS-GMLC/` (public RTS-GMLC dataset + 25.6 MB PDF, PLEXOS reports);
  - `data/Heterogeneous/investors/investor{1..4}/gen.xlsx` (project investor
    portfolios, deleted `730d79d` 2024-08-05);
  - `data/RTS-GMLC_No_VRE_Reserves/`, `config/legacy_ercot/`,
    `other_dependency/PRAS/`.
- **Bundled files whose releasability is undecided:**
  - `src/resource_adequacy/descriptors/outage-rates-ERCOT-modified.csv` (FOR/MTTR
    by prime mover, fuel, and size class; looks GADS-derived; added by yinongsun
    2022);
  - `cc_restrictions.json`;
  - `config/ercot_est/` (RTS generator names; public);
  - `config/investor_defaults/` (63 files) and `config/project_defaults.csv`
    (added by Purboday Sep 2026, provenance unstated).
- **Internal paths and markers in `src/` and `test/`:**
  - `definitions.jl:33` (`TIMESERIES_DATA_DIR`; only use is a commented line,
    `rts_psy_creator.jl:62`);
  - `siip_simulation_definition.jl:1528-1530` (commented `to_json` to `/kfs2/...`);
  - `agent_simulation_creator.jl:67` and `rts_reader.jl:424` (`NY_change`
    comments with paths);
  - `save_load_utils.jl:2298`;
  - `test/test_seasonal_capacity_markets.jl:5`;
  - 13 `NY_change` markers across 7 files.
- **Untracked files in the working copy:**
  - `config/project_templates/gs_ercot_reference/`: header-only mirror of
    GS_AAYAD; `outage/ThermalFOR_2011.csv` lists every real generator name;
    the README cites production paths;
  - `seasonal_capacity_markets_plan.md`, owned by pghosh;
  - this file.

### 3.6 Settings-key mismatch with GS_AAYAD (breaks GS cases, found statically)

`CaseDefinition.jl:496` reads `simulation_settings["md_market"]` with no alias
(since `1316cbe`, 2026-04-20). Every GS_AAYAD `simulation_settings.csv` (3 cases
+ `template_run`) defines `multi_day_market` instead. `derating_scale` exists
only in GS settings and is read nowhere (commented out, `CaseDefinition.jl:134`).
`cd2ba58` already fixed the `accreditation_methodology` default (`TopNetLoad`).
*Not executed:* inferred from code; a GS case load would raise `KeyError`.

### 3.7 `pg/rts-gmlc-test-system` (verified 2026-09-30 in an isolated worktree)

What it adds:

- `config/example_systems/rts_gmlc/project_spec/`: a complete example project
  (241 files, **60.8 MB**). It contains:
  - a 3-area zonal RTS-GMLC PSY system (`rts_gmlc_zonal_sys.json` + 3.7 MB `.h5`);
  - 6 sim-years of DA/RT timeseries (Load, WIND, PV, Hydro, Reserves);
  - `ThermalFOR_2020.csv` (3.3 MB);
  - 4 investors with market data, finance, capex, and derating inputs.
- Code changes, all small:
  - `validate_existing_project_lifetimes` (new, tested);
  - `_validate_derating_columns` (new, wired into `validate_emis_project`);
  - `normalize_legacy_reserve_services!` (replaces 2 copies of hardcoded
    SPIN/NONSPIN/REG_* edits; now skips absent services instead of erroring);
  - a storage `Duration Hr` per-unit fix in `system_extractor.jl`;
  - 2 rows in `psy_classification_mapping.csv` (oil ST/CT);
  - `read_availability_df` → `get_availability_df` in
    `derating_factor_calculator.jl:118`.

Verified results:

| Check | Result |
|---|---|
| Merge into `aa/set_system_inputs` | Clean (`git merge-tree`), no conflicts. |
| Project-init test files on the branch | 99/99 pass (96 + 3 new lifetime tests). |
| **`read_availability_df`** | **Defined nowhere on `aa/set_system_inputs`.** `calculate_derating_data` would throw `UndefVarError` there. The branch fixes it, so this branch needs that commit. |
| Zone mapping (§3.1 method) | **Correct.** Area `1`/`2`/`3` correlates 1.000 with CSV column `1`/`2`/`3`; every other column ≤ 0.69. The magnitude method can't tell these apart (all 3 loads peak at 2850 MW); only the hourly method works here. |
| `initialize_emis_project` on the example as committed | **Fails.** The branch's own new lifetime check rejects `313_STORAGE_1` (Online Year 2000 from the `ALL,Online Year,2000` default + `BA,Lifetime,15` → expired 2015 < `start_year` 2020). The example's `projectexisting.csv` has no per-unit `Online Year` override. Consistent with "not final". |
| Init with that one override added (scratch copy) | Succeeds, `validate_emis_project` passes. But the validator's `ts_root` doesn't exist (§3.2 bug), and **renaming load column `3`→`99` still passes validation.** |
| Calendar | Every sim-year's CSVs are labeled 2020-01-01..2020-12-31, 8760 rows, **Feb 29 dropped**. `start_year`/`base_load_year`/`weather_year` = 2020. But the PSY system's forecasts start **2018-01-01** (`SIM_START_DATE`). The example carries two calendars, and 2020 is a leap year, i.e. the §3.4 trap. |
| Duplication | WIND files for sim-years 1-6 are byte-identical (same md5). Load differs per year. Most of the 60.8 MB is copies that could be generated. |
| Release hygiene | `project_spec.csv` contains `scratch_dir,/scratch/pghosh`. No README / LICENSE / attribution for the RTS-GMLC data in `config/example_systems/`. |
| Precompilation | Same §2 `PSY.PowerSystemTableData` overwrite error. Unchanged. |
| Not checked | A simulation run of the example (only init + validate were run). |

What this means for the plan:

- It **is the second system** that old step 6 asked for: non-ERCOT, 3 zones,
  numeric area names, different technology mix, and a verified-correct zone
  mapping. The plan should use it rather than build a separate synthetic system.
- It **makes D1/D2 urgent**. If 60.8 MB of data lands in any branch that
  becomes the public repo's history, it can only be removed by rewriting
  history.

## 4. Next steps

Order matters: decisions (step 1) and environment isolation (step 2) unblock
everything else. Step 3 merges in the RTS-GMLC test system, which then becomes
the acceptance fixture for every later step, so fixes are verified against a
non-ERCOT system and not just against ERCOT still passing.

### Step 1: Decisions needed

| # | Question | Evidence so far |
|---|---|---|
| D1 | Publish as a **fresh squashed repo**, or **rewrite history** (`git filter-repo`)? | §3.5. Fresh repo is simpler and cannot leak; a rewrite keeps authorship but invalidates all clones/branches. |
| D2 | For each bundled file in §3.5: releasable as is, replace with a synthetic default, or remove? Mainly `outage-rates-ERCOT-modified.csv` and `investor_defaults/`. **Add:** RTS-GMLC example data (public dataset; confirm its license and add attribution) and `ThermalFOR_2020.csv` (provenance not stated). | §3.5, §3.7. |
| D3 | Adopt the derived ERCOT zone mapping (§3.1)? This **changes production results** (load-growth assignment). Confirm `annual_growth.csv` column semantics first. | §3.1 table. |
| D4 | Keep the `ordc_unavailability_method = "SMC"` path (legacy `make_pras_system`), or deprecate it in favor of SPI? | §3.3: no GS case and not the RTS example use it. Deprecating deletes the import-time outage file and most of `PSY2PRAS.jl`. |
| D5 | Keep supporting the legacy `input_processing/` raw-data construction path (`project_init/README.md` §"legacy")? | Not audited. |
| D6 | Accept `multi_day_market` as an alias for `md_market`, or migrate GS settings files? | §3.6. The RTS example already uses `md_market`. |
| D7 | Where do `gs_ercot_reference/` and `seasonal_capacity_markets_plan.md` live? In this repo (internal only), outside it, or dropped? | §3.5. |
| D8 | Is the `PENALTY_COST` (5000, here) vs. `energy_penalty` (1e4, EMISPostProcessing.jl) mismatch in scope? | Unchanged from 09-18 draft. |
| **D9** | **Where does example/test data live?** Options: (a) commit in-repo (current branch: 60.8 MB); (b) Git LFS; (c) **Julia `Artifacts.toml`** pointing at a versioned tarball (GitHub release or separate data repo), downloaded on demand by tests/examples; (d) commit only sim-year 1 + small inputs and *generate* years 2-6. **Recommendation: (c) + (d).** The package repo stays code-only (which is the stated goal); CI and users get the data reproducibly; nothing large enters history. Must be decided **before** `pg/rts-gmlc-test-system` merges anywhere that will be published. | §3.7: 60.8 MB, duplicated sim-years, binary `.h5`. |

### Step 2: Isolate development from production

- Production (GS_AAYAD HPC env) dev-links this checkout. Point it at a separate
  checkout or `git worktree` pinned to a known-good commit, so switching branches
  here can no longer change running jobs.
- Resolve the production env's stale manifest (`JSON` missing) only when
  production intentionally moves to a PR #51-based commit.
- Add a committed `test/Project.toml` (or document the env), so tests run
  without borrowing the production env. The repo-root env currently isn't
  instantiated (`AxisArrays` missing).
- **Done when:** `julia --project=<dev env> test/runtests.jl` runs from a fresh
  clone.

### Step 3: Land the RTS-GMLC test system (coordinate with Purboday)

Owner: Purboday (branch author). This plan only lists what it needs before
merging into `aa/set_system_inputs`.

- **Must fix before merge:**
  - make the example initialize: add per-unit `Online Year` to
    `projectexisting.csv`, or fix the `ALL,Online Year,2000` default. Today it
    fails its own lifetime check (§3.7);
  - remove `scratch_dir,/scratch/pghosh` from `project_spec.csv`;
  - store the data per D9. Ideally restructure the commit before it reaches
    `aa/set_system_inputs`, so the 60.8 MB never enters this branch's history;
  - add a README to `config/example_systems/rts_gmlc/` with RTS-GMLC source,
    license, and attribution (D2), and state which calendar year the data
    represents.
- **Keep as is:** the `get_availability_df` fix (needed by this branch, §3.7),
  `normalize_legacy_reserve_services!`, the two new validators, and the storage
  duration fix.
- **Add a test** `test/test_example_rts_gmlc.jl`: `initialize_emis_project` →
  `validate_emis_project` on the example (fetched via the artifact if D9 = c).
  This is the regression bar for every later step.
- **Done when:** the merge is clean (verified possible today) and the new test
  passes.

### Step 4: Zone mapping (after D3)

- **CI ground-truth test on RTS-GMLC.** Commit a function that derives
  *(area → load column)* from a PSY system + regional load CSV by hourly
  correlation (§6 method). Assert it equals the example's `zones.csv`.
  Verified today: 3/3, correlation 1.000 vs. ≤ 0.69. Do **not** use the
  peak-share method alone: it can't tell apart RTS-GMLC's equal-peak areas.
- **ERCOT legacy default.** Update `config/ercot_est/system_config/zones.csv` to
  the derived mapping (§3.1). Replace the pinned assertions in
  `test_system_config.jl:64-75` with an opt-in test of the same function, run
  when `EMIS_GROUND_TRUTH_SYS` points at the (git-ignored) GS system.
- Remove the positional `zone_$(index)` fallback in `find_zonal_area` (proven
  wrong, §3.1). Require a `SystemConfig`, or error clearly.
- Flag the results change for GS runs in the PR description, separately.
- **Done when:** the RTS test passes, and the opt-in ERCOT test fails on the old
  `zones.csv` and passes on the new one.

### Step 5: Fix the validator and remove the PSY method overrides

- `_base_project_paths`: read `timeseries_data_dir` from `project_metadata.csv`.
  **Test:** on the RTS example, renaming a load column (`3`→`99`) must fail
  validation. Verified that it passes today.
- `_validate_psy_generator_names`: implement the check (each VRE column must
  name a generator in the system) or delete it. **Test:** a bogus WIND column
  in the RTS example must raise.
- Once the timeseries are visible to the validator, check that the branch's new
  `_validate_derating_columns` and the existing checks still pass on the RTS
  example. Until then they were only exercised on the template.
- Replace `thermal_types` with `class == "thermal"` from `technologies.csv`.
  The RTS example's `technologies.csv` already has `class` for every type.
- Move `add_outage_info!` + `transform_outage_to_rate` into `ra_utils.jl`.
  Delete the rest of `parsers/power_system_table_data.jl` and
  `parsers/descriptors/`.
  **Test:** the package precompiles with no `Method overwriting` error, and
  existing tests still pass.

### Step 6: Calendar (the RTS example is the test case)

- Split `SIM_START_DATE` into:
  - an internal constant `DATA_REFERENCE_YEAR` (fixed non-leap, used only by
    `season_hour_columns`), and
  - a configurable timestamp start, from `start_year` in `project_spec.csv`
    (used to label PSY/PRAS time series in `siip_psy_utils.jl` /
    `rts_psy_creator.jl`).
- Fix the RTS example so its PSY system timestamps and CSV labels agree.
  Today they are 2018 vs. 2020 (§3.7).
- Add validation for the data contract: 8760 rows per `sim_year_N` file, no
  Feb 29, `Period` in 1-24. The RTS example already conforms. Document that
  leap-year data must drop Feb 29, as the example does.
- **Tests:**
  - `season_hour_columns` output is identical for start year 2018 and 2020 (the
    RTS example's year);
  - an 8784-row load file is rejected.

### Step 7: `resource_adequacy/` onto `SystemConfig` (scope depends on D4)

- If SMC is kept: stop reading `OUTAGE_INFO_FILE` at import. Load it lazily
  inside `make_pras_system` from a path registered in `input_manifest.jl`, with
  the bundled file as the default. Add existence, schema, and referential tests
  (reuse `_require_columns`).
- If SMC is deprecated: delete `make_pras_system` and its callers, the import-time
  load, and `descriptors/outage-rates-ERCOT-modified.csv`.
- The RTS example supplies its own `outage_filepath` (`ThermalFOR_2020.csv`).
  Use it to test the user-supplied outage path end to end.
- Check against both ERCOT and the RTS example.

### Step 8: Settings compatibility (after D6)

- Resolve `md_market` / `multi_day_market` (§3.6) and remove or implement
  `derating_scale`.
- **Test:** each GS `simulation_settings.csv` and the RTS example's
  `simulation_settings.csv` load into a `CaseDefinition`.

### Step 9: End-to-end run on the RTS example

- A short (1-2 sim-year) simulation of the RTS example in CI or as a
  documented manual check. This is what turns "system-agnostic" from a claim
  into a tested fact. Not attempted yet.
- Optional, after this passes: a tiny synthetic system (e.g. 2 zones,
  non-numeric zone names, non-contiguous IDs) as a cheaper unit-level fixture.
  It is no longer the acceptance bar.

### Step 10: Release cleanup

- Remove every §3.5 path/marker item, the §3.7 `/scratch/pghosh` path, and dead
  `TIMESERIES_DATA_DIR`/`POINTER_FILE`.
- Rename the hardcoded `"EMIS_RTS_Analysis"`/`"RTS-GMLC"` layout folders
  (§3.2) or make them `project_spec` fields. The RTS example already passes
  them as `base_dir_name` / `test_system_dir_name`, so the code defaults are
  what's left.
- Collect scattered internal filename literals (`"load_n_vg_data.csv"`,
  `"reserve_products.csv"`, …) as constants in `definitions.jl`.
- Apply D2 labels. Write a data manifest (path, provenance, license, label) for
  every tracked non-code file.
- Add a CI release gate that fails if:
  - a tracked data file is not in the manifest allowlist;
  - a tracked file is over a size cap (e.g. 1 MB) unless allowlisted;
  - `/kfs2`, `/projects`, `/scratch`, or `/home` appears in tracked files;
  - `test/test_systems/` is tracked.
- Write the user guide for the input format in `src/project_init/README.md`,
  using the RTS example as the worked example.
- Publish per D1.

## 5. Testing rules (carried from the 09-18 draft; rule 5 updated)

1. Existence + parse test for every bundled default file.
2. Schema test via the existing `_require_columns` / `_require_nonempty_column`.
3. Referential-integrity test for every value that names another entity.
4. Ground-truth test for anything that claims to describe a real system
   (derive the answer from the data, not from the config).
5. The RTS-GMLC example (step 3) is the acceptance bar for "system-agnostic":
   init + validate now (step 3), a short end-to-end run later (step 9).
6. Every silent fallback (warn-and-skip) gets a test that triggers it and
   asserts the documented behavior.

## 6. Reproducing the verification

- **Environment.** Copy
  `/projects/gmlcmarkets/Phase2_EMIS_Analysis/GS_AAYAD/HPC_Analysis_Runs/{Project,Manifest}.toml`
  to a scratch dir. Point the `EMISAgentSimulation` `path` at this checkout, then
  run `Pkg.resolve(); Pkg.instantiate()` *in the copy*. Never touch the
  production manifest.
- **Zone mapping.** Load `test/test_systems/sys_UC_year1.json` with PSY. For each
  `Area`, sum `max_active_power` `Deterministic` forecasts over its loads
  (24 h × 60 windows from `initial_timestamp`). Correlate against each column of
  the regional load CSV. Cross-check with shares of `max_active_power` vs.
  column peaks.
- **Validator.** `initialize_emis_project(config/project_templates/project_spec)`
  → add a bogus column to a WIND csv under the validator's `ts_root` →
  `validate_emis_project` still passes.
- **Calendar.** Compare `month(DateTime(y) + Hour(h))` for h in 0:8759, for
  y = 2018 vs. 2020.
- **RTS-GMLC branch.** `git worktree add --detach <scratch>/wt_rts
  origin/pg/rts-gmlc-test-system`. Copy the scratch env and point its
  `EMISAgentSimulation` `path` at the worktree. Run the same zone method on
  `config/example_systems/rts_gmlc/project_spec/rts_gmlc_zonal_sys.json`
  (60 windows). For init/validate, use a scratch copy of the spec with an
  `Online Year` column (`313_STORAGE_1` = 2015). Remove the worktree after:
  `git worktree remove <scratch>/wt_rts`.
