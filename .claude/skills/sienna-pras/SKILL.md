---
name: sienna-pras
description: Verified facts for resource adequacy and capacity accreditation in EMIS — PSY→PRAS conversion (which units, which capacities), the TopNetLoad and ELCC derating calculations, and how year-1 derating seeds are used. Use before changing or debugging PRAS systems, derating_dict.csv files, capacity credits or accreditation in EMIS.
---

# PRAS and capacity accreditation in EMIS — verified knowledge

Every entry was checked against code or data. Format: **claim** — evidence — date. Add new findings here in the same
turn they are found; mark unconfirmed items *hypothesis*; correct wrong entries explicitly.

Code paths are relative to `EMISAgentSimulation.jl/src/`, branch `aa/Claude` @ 2fe9c10; re-grep line numbers.

## 1. PSY → PRAS

- **Generator capacity in PRAS = forecast × `get_max_active_power(g)`** for units with a `max_active_power` series,
  else a flat `get_max_active_power(g)` (`resource_adequacy/PSY2PRAS.jl:575-602`). For wind/PV the profile ÷ rating ×
  rating cancels, so a wrong wind/PV rating does not change PRAS capacity. — 2026-11-14
- **Only available components are converted** (`PSY2PRAS.jl:77-85`, `get_available_components_in_aggregation_topology`).
  — 2026-11-15

## 2. Derating (capacity credit) calculation

- **TopNetLoad, existing wind/PV** (`derating_factor_updates/derating_factor_calculator.jl:18-125`,
  `calculate_derating_data`), per scenario:
  ```
  load(h)      = Σ zone load columns of Net Load Data/load_n_vg_data_rt.csv, years 1..total_horizon
  net_load(h)  = load(h) − Σ existing wind/PV MW
  for tech-zone k:  wo_k = net_load + Σ_{g∈k} MW_g
                    top  = num_top_hours (Capacity.csv) × total_horizon hours with the largest wo_k
                    existing_k = min( Σ_top (wo_k − net_load) / Σ_{g∈k} get_maxcap(g) / n_top , 1.0 )
  ```
  The numerator uses raw MW profiles and does not depend on unit sizes; the denominator is the sum of sizes. NaN for
  tech-zones with no units. Uses the **full `total_horizon`**, not the current year. — 2026-11-14
- `load_n_vg_data_rt.csv` is written by `load_n_vg_data` (`test_system_parsers/rts_reader.jl:92-132, 300-312`) from the
  canonical `REAL_TIME_regional_Load.csv` zone columns (unscaled, leap day removed) and the canonical RT wind/PV MW
  columns, so the formula can be reproduced from the spec's canonical files. Independent re-implementation
  (`HPC_Analysis_Runs/ercot_ordc/derating_seeds/existing_vre_derating.jl formula`) matched a Python version to 2e-16.
  Not yet compared with the package's own output (pending the 1-year smoke run). — 2026-11-14
- **ELCC/EFC path** also divides by `total_capacity = Σ get_maxcap` per tech-zone (`derating_factor_calculator.jl:587-604`).
  — 2026-11-14
- Per-type scalars are applied later in `update_derating_factor!`; `derating_dict.csv` holds raw values. — docstring at
  `derating_factor_calculator.jl:14-17`

## 3. Year-1 derating seeds

- **`markets_data/derating_data/<scenario>/derating_dict.csv` is used as-is in year 1**: read at setup
  (`struct_creators/simulation_structs/agent_simulation_creator.jl:190-194`) and saved as the year-1 derating
  (`investment_simulation.jl:144-165`). **From year 2 the calculator overwrites it** (`investment_simulation.jl:462-485`,
  via `parallelize_update_derating_data` → `update_simulation_derating_data!`). Year-1 capacity market and investor
  decisions therefore depend on the seed. — 2026-11-14
- Project init writes a seed only for scenarios without one (`_seed_derating_templates`); wind/PV columns get **0.0**
  because `config/investor_defaults/derating_baseline.csv` lists no wind/PV types. — 2026-11-14
- **The GS ERCOT seeds (`template_run/scenario_data/markets_data/derating_data/`, dated Jul 12) are not reproducible
  from the GS data:** recomputing with the stored (wrong) sizes gives PV 0.001–0.005, not the seeds' 0.005–0.13. Do not
  reuse them with corrected sizes (year-1 PV credit would be ~1 % of nameplate). — 2026-11-14
