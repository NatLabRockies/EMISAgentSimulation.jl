---
name: sienna-psy
description: Verified facts and checks for working with PowerSystems.jl (PSY) / PowerSimulations.jl (PSI) systems in EMIS — unit sizes and units, wind/PV ratings, time series in canonical builds, unavailable components, reserve names, saving and diffing systems, and known data problems in the GS ERCOT system. Use before reading, modifying, saving, comparing or debugging any PSY system (JSON + time-series .h5) or a Sienna UC/ED/MD build.
---

# Sienna (PSY / PSI) — verified knowledge

Every entry here was checked against code or data. Format: **claim** — evidence — date. Items marked *hypothesis* are
not confirmed. When a new PSY/PSI issue is found, add it here in the same turn (claim, evidence, date); correct entries
that turn out wrong instead of deleting them silently.

Code paths are relative to `EMISAgentSimulation.jl/src/`. Line numbers are as of branch `aa/Claude` @ 2fe9c10 (the
`system_extractor.jl` base_power fix is committed only on `aa/ordc_tests` @ 0c204b1); re-grep before relying on them.

## 1. Units and sizes

- **Unit size in MW = `rating × base_power`; `rating` is per unit of the component's own `base_power` (device base).**
  Getters return values in the system's current unit setting (`SYSTEM_BASE` for the GS/RTS files, i.e. per unit of the
  100 MW system base). To read MW, call `PSY.set_units_base_system!(sys, "NATURAL_UNITS")` first. — verified with
  `ercot_ordc/verify_vre_fix.jl` — 2026-11-15
- **Do not compute sizes from raw JSON fields.** The JSON stores device-base values; mixing them with system-base
  assumptions gave impossible numbers (e.g. 1,486,076 MW for one PV unit). Load the system with PSY and use getters. —
  2026-11-14
- **Wind/PV (`RenewableDispatch`) has no `active_power_limits`.** Its maximum is `PSY.get_max_active_power(g)` =
  `rating × power_factor`. Code that calls `get_active_power_limits` on all generators throws a `MethodError` for
  wind/PV. — `project_init/system_extractor.jl:109-125` (`_component_capacity_mw`) and `extract_fleet` (~:640-646) —
  2026-11-15

How to check sizes (MW) for wind/PV:
```julia
using PowerSystems; const PSY = PowerSystems
sys = PSY.System(path; runchecks = false, time_series_directory = ENV["TMPDIR"])
PSY.set_units_base_system!(sys, "NATURAL_UNITS")
for g in PSY.get_components(PSY.RenewableDispatch, sys)
    println(PSY.get_name(g), " ", PSY.get_rating(g), " MW (base_power ", PSY.get_base_power(g), ")")
end
```

## 2. Wind/PV time series and why wrong ratings hide in dispatch

- **Sienna dispatch is insensitive to the wind/PV rating.** The build divides the MW profile by `get_rating(d)`
  (`struct_creators/simulation_structs/rts_psy_creator.jl:768`) and attaches it with
  `scaling_factor_multiplier = get_max_active_power` (:806), so available MW = profile MW whatever the rating.
  A wrong rating only shows in quantities that use the size directly (EMIS fixed O&M, derating, capacity market, see
  the `emis` skill). — 2026-11-14
- **A sanity check that does catch wrong ratings: profile peak / size ≤ 1.** Correct units peak below size (RTS:
  0.86–0.99; ERCOT after fix: PV 1.000, wind 0.65–0.85). — `ercot_ordc/verify_vre_fix.jl` step 4 — 2026-11-15
- **Canonical builds delete every existing forecast first.** `create_sys_w_updated_ts` calls
  `remove_time_series!(sys_MD, Deterministic)` and `DeterministicSingleTimeSeries` (`rts_psy_creator.jl:658-659`)
  before attaching the canonical files. Consequences:
  - a wind/PV unit with no matching column in `WIND/` or `PV/` files gets **no** `max_active_power` series (a warning,
    then skipped);
  - a reserve with no `<STAGE>_regional_<product>.csv` gets no requirement series; with only `DAY_AHEAD_*` files the
    ED (real-time) systems have **no** Reg_Up/Reg_Down requirement. Provide `REAL_TIME_regional_*` files.
  - Hydro is rebuilt from the base system's own series (`HydroDispatch` reconstruction just before :658), not from
    canonical `Hydro/` files.
  — 2026-11-14
- Canonical vs legacy mode: canonical when `<time_series_data_dir>/input_processing/` does **not** exist
  (`rts_psy_creator.jl:88`, `use_canonical_load = !isdir(ntp_ts_data_dir)`). — 2026-11-14

## 3. Unavailable components

- **PSI builds only available components** (`get_available_components` in the renewable/thermal constructors), and PRAS
  conversion filters on `PSY.get_available` (`resource_adequacy/PSY2PRAS.jl:85`). An `available = false` unit is inert
  in dispatch and adequacy even without time series. — 2026-11-15
- **Project init treats them inconsistently:** `extract_gen_table` skips unavailable units (they are absent from
  `gen.csv`), while `extract_fleet` **errors** on an owned unavailable unit
  (`Existing project "<name>" is not an available PSY Generator/Storage component`). Remove such units from the
  ownership list. — GS ERCOT `gen-543` — 2026-11-15
- Calling `PSY.get_available` over all `PSY.Component` types fails (e.g. `Arc` has no `available`); restrict to
  `PSY.Generator` and `PSY.Storage`. — 2026-11-15

## 4. Reserves

- **ERCOT GS reserve services are named `REG_UP`, `REG_DN`, `SPIN`, `NONSPIN`;** EMIS markets and the PSI service models
  use `Reg_Up` / `Reg_Down`. Already handled: `RESERVE_PRODUCT_ALIASES` (`definitions.jl:6`) maps `REG_DN→Reg_Down`,
  `REG_UP→Reg_Up` for file matching (`_resolve_reserve_product_name`, `system_extractor.jl:409`;
  `_canonical_reserve_file`, `rts_psy_creator.jl:487`), and `normalize_legacy_reserve_services!` renames REG_* and removes
  SPIN/NONSPIN on every MD/UC/ED system (`rts_psy_creator.jl:394`). Do not add another alias table. — 2026-11-14
- **Reserve requirement series are scale factors:** the stored value must be `MW / (get_requirement × system base power)`
  because PSI multiplies by `get_requirement` (in system per unit). Use `reserve_requirement_scaling_factors`
  (`utils/siip_psy_utils.jl:12`). — tested in `test/test_reserve_requirement_units.jl`

## 5. Loading, saving, diffing

- `PSY.System(path)` copies the time-series store to `time_series_directory` (default system temp). Pass a scratch
  directory (`time_series_directory = ENV["TMPDIR"]`) on HPC. — 2026-11-14
- **Re-saving with `PSY.to_json` adds fields the current serializer writes by default** (e.g. `TapTransformer`:
  `number_of_tap_positions`, `regulated_bus_number`, `tap_limits`, `voltage_setpoint` when the source file predates
  them). To diff a modified system against its source, first re-save the source with the same PSY version and compare
  those two; then only real changes remain. — `ercot_ordc/verify_vre_fix.jl` step 1 — 2026-11-15
- Modify systems through the PSY API (`set_rating!`, `set_active_power!`, …) and save with `to_json`, not by editing the
  JSON, so the JSON and its `_time_series_storage.h5` stay consistent. Set `DEVICE_BASE` before setting per-unit fields,
  and restore the file's unit setting before saving. — `ercot_ordc/fix_vre_ratings.jl` — 2026-11-15

## 6. Known data problems: GS ERCOT system (`RTS-GMLC_GS/DA_sys_zonal*.json`, also in its `constructed_systems/`)

- **PV `rating` holds MW instead of per unit** (131/132 units): size = rating × base_power ≈ MW² (Sisters Solar
  1,486,076 MW; total ≈ 7.5 million MW). Profiles peak at exactly `base_power`. — 2026-11-14
- **Wind sizes are placeholders**: rating × base_power = 65.0 MW (81 units) or 85.2 MW (5), whatever the unit
  (MIAMI_1 base 346 MW → 65; RALLS_1 base 12 MW → 65). Profiles peak at exactly 0.65 / 0.852 × `base_power`.
  *Hypothesis:* the peak capacity factor was stored as a system-base rating (0.65 × 100 MW). — 2026-11-14
- Fix used for the ERCOT spec: `rating = 1.0` for all WT/PVe (size = base_power), via
  `HPC_Analysis_Runs/ercot_ordc/fix_vre_ratings.jl` → `project_spec/ercot_gs_vre_fixed.json`; verified by
  `verify_vre_fix.jl` (only `rating` changed on 218 units; all checks pass). GS production data were not changed.
  — 2026-11-15
- `Glove Solar` (100 MW PV) and `gen-543` (105 MW gas GT) are `available = false`. — 2026-11-15
