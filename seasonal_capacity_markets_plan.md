# Plan: Seasonal Capacity Markets (Summer / Winter)

## Overview

Add a seasonal dimension to capacity markets so that EMIS can model separate summer and winter
capacity obligations, as in ISO-NE's planned market redesign. The feature is toggled via a single
Boolean flag in `CaseDefinition`. When disabled, every code path is identical to today — no
performance cost, no file format changes required for existing cases.

The core design principle: treat seasons as a data-driven list of strings (`["annual"]` or
`["summer", "winter"]`). All capacity loops iterate over this list. A single-element `["annual"]`
list degrades exactly to the current annual behavior.

---

## Data Files and Their Contents

### Input files (read by the simulation)

| File | Path | Contents | Who reads it |
|------|------|----------|--------------|
| **`capacity_seasons.csv`** *(new)* | `{base_dir}/Heterogeneous/markets_data/capacity_seasons.csv` | **Canonical season definition.** Columns: `name` (primary key), `months` (e.g. `"May-Sep"`). One row per season. All other files and structs that reference a season use the `name` value from this file. Absent = annual-only fallback. | `cem_creator.jl`, `actual_market_simulation.jl`, `derating_factor_calculator.jl` |
| **`Capacity.csv` (system-level)** | `{base_dir}/Heterogeneous/markets_data/Capacity.csv` | Market design parameters for **actual clearing** per season: IRM, EFORd, Net/Gross CONE and breakpoints, `Max Clear`, `introduction_year`, `discontinuation_year`. `season` column is a **foreign key into `capacity_seasons.csv`**. One row today (no `season` column); one row per season in seasonal mode. | `actual_market_simulation.jl` |
| **`Capacity.csv` (investor-level)** | `{base_dir}/Heterogeneous/investors/{investorX}/markets_data/Capacity.csv` | Same schema as the system-level file but **per-investor** — each investor may have their own view of market parameters for price prediction. `season` column is a foreign key into the system-level `capacity_seasons.csv`. | `cem_creator.jl` |
| **`derating_dict.csv`** | `{base_dir}/markets_data/derating_data/{scenario}/derating_dict.csv` | Output of the derating calculator. Columns: `season` (foreign key into `capacity_seasons.csv`), `Thermal_CC`, `existing_Wind_zone_1`, `new_Wind_zone_1`, `existing_STOR_4`, … One row per season; one row total in annual mode (no `season` column). | `update_derating_factor!` functions in `derating_factor_calculator.jl` |
| **Simulation settings** | `CaseDefinition` struct or serialized HDF5 | All simulation-wide toggles including `static_capacity_market`, `irm_scalar`. **New field `seasonal_capacity_market` added here.** | `save_load_utils.jl` (serialize/deserialize); used everywhere via `get_case(simulation)` |
| **Expected market data HDF5** | `{investor_dir}/expected_market_data/{scenario}_year_{year}.h5` | CEM output per investor per year: `capacity_price`, `capacity_accepted_perc`, energy/reserve prices, etc. `capacity_price` group uses season `name` as sub-group key in seasonal mode. | Written by `expected_market_simulation.jl`; read by `investor_iteration.jl` → `capacity_profit.jl` |

### New file: `capacity_seasons.csv` — canonical season definition

**Path:** `{base_dir}/Heterogeneous/markets_data/capacity_seasons.csv`  
**Who reads it:** derating calculator, `cem_creator.jl`, `actual_market_simulation.jl` — every
component that needs to know which months belong to which season.

The season `name` column is the **primary key** referenced by all other files and data
structures. Month ranges use the same `"MMM-MMM"` format and `month_lookup` helper already
used by the ORDC construction (`src/utils/general.jl` line 279).

```
name,      months
summer,    May-Sep
winter,    Oct-Apr
```

Or with shoulder seasons:

```
name,      months
summer,    Jun-Aug
spring,    Mar-May
fall,      Sep-Nov
winter,    Dec-Feb
```

Annual mode (single annual market, no file changes to anything else):

```
name,    months
annual,  Jan-Dec
```

**Rules:**
- `months` is a `"MMM-MMM"` range. Wrap-around (e.g. `Oct-Apr`) is supported using the
  existing `month_lookup` logic from `ordc_construction.jl` — start > end means the range
  wraps through December into January.
- Month abbreviations use the `month_lookup` function already in `src/utils/general.jl`.
- Adding a third season (e.g. `shoulder`) requires only one new row here; all downstream
  loops iterate over `keys(season_months)` and pick it up automatically.
- In annual mode this file has a single row `annual, Jan-Dec`. Existing cases that do not
  create this file can be handled by a fallback that constructs
  `Dict("annual" => collect(1:12))` when the file is absent.

### Changes to `Capacity.csv`

The `season_months` column is **removed from `Capacity.csv`**. The `season` column is kept
as the foreign key into `capacity_seasons.csv`.

**Annual mode — no file changes required.** Existing single-row files (no `season` column)
continue to work; when `capacity_seasons.csv` is absent the code defaults to annual mode.

**Seasonal mode — `Capacity.csv` format:**

```
season,  introduction_year,  discontinuation_year,  IRM,   EFORd,  Net CONE per day,  ...
summer,  2024,               2050,                  0.15,  0.08,   40.0,              ...
winter,  2024,               2050,                  0.17,  0.10,   45.0,              ...
```

`introduction_year` and `discontinuation_year` are read from the first row only (they are
market-wide, not season-specific). All other parameters (IRM, CONE, EFORd, breakpoints) are
per-season so each season can have its own demand curve shape.

---

## Phase 1 — Struct & Type Foundation
*Both developers work on this together before splitting. All downstream phases depend on it.*

### 1a. `CaseDefinition` — simulation-wide toggle

**File:** `src/structs/CaseDefinition.jl`  
**Module role:** Central struct that holds every simulation-wide setting. Passed through
`AgentSimulation` to all subsystems via `get_case(simulation)`. All Boolean flags that switch
market features on/off live here (e.g. `static_capacity_market`, `mopr`, `battery_cap_mkt`).

**Change:** Add one field, following the exact pattern of `static_capacity_market::Bool`:

```julia
seasonal_capacity_market::Bool
```

- Add to the field list, the inner constructor argument list, the `new(...)` call, and the
  `@assert irm_scalar >= 0.0` block (no assert needed — it is a Bool).
- Also add corresponding getter `get_seasonal_capacity_market(c::CaseDefinition) = c.seasonal_capacity_market`.

**Also update:** `src/utils/save_load_utils.jl` — `save_case_definition!` and
`load_case_definition` (around lines 85 and 141) serialize/deserialize every `CaseDefinition`
field to HDF5. Add:
```julia
# in save_case_definition!
write(g, "seasonal_capacity_market", get_seasonal_capacity_market(c))

# in load_case_definition
read(g, "seasonal_capacity_market"),   # in the positional CaseDefinition(...) call
```

---

### 1b. `Capacity` product struct — per-project market participation data

**File:** `src/structs/products/Capacity.jl`  
**Module role:** One `Capacity` instance is attached to each capacity-eligible project.
Stores the project's derating factor (how much of its nameplate capacity counts toward the
capacity requirement) and its historically accepted percentage (used in revenue forecasting).
Multiple dispatch methods on `Product` mean only `Capacity`-typed products participate in
capacity market logic.

**Changes:**

```julia
# Before
derating::Dict{String, Float64}               # scenario → derating
accepted_perc::Dict{String, Vector{Float64}}  # scenario → yearly vector

# After
derating::Dict{String, Dict{String, Float64}}               # scenario → season → derating
accepted_perc::Dict{String, Dict{String, Vector{Float64}}}  # scenario → season → yearly vector
```

Update all dispatch methods to add a `season::String` argument:
- `get_derating(prod::Capacity, scenario, season)` — new primary form
- `get_derating(prod::Capacity, scenario)` — backward-compat wrapper: returns `prod.derating[scenario]["annual"]`
- Same pattern for `set_derating!`, `get_accepted_perc`, `set_accepted_perc!`

In annual mode, the single season key is always `"annual"`.

---

### 1c. `MarketProject` struct — project representation inside the CEM optimizer

**File:** `src/structs/market_structs/MarketProject.jl`  
**Module role:** A lightweight, optimization-ready representation of a project used solely
inside `cem.jl`. Contains all scalar parameters the JuMP model needs: cost coefficients,
capacity, availability, derating. Created by `market_project_creator.jl` from full `Project`
objects, deliberately stripped of investor/finance data.

**Change:**

```julia
# Before
derating_factor::Float64

# After
derating_factor::Dict{String, Float64}   # season → value
```

Update the inner constructor: replace `@assert derating_factor >= 0` with
`@assert all(values(derating_factor) .>= 0)`.

---

### 1d. `MarketCollection` struct — per-investment-period market bundle

**File:** `src/structs/market_structs/MarketCollection.jl`  
**Module role:** Groups all market structs for one investment period into a single object
passed to `cem.jl`. The `MarketClearingProblem` struct holds a `Vector{MarketCollection}`,
one entry per investment period (year). Already uses `Dict{String, ReserveUpMarket}`,
`Dict{String, ReserveDownMarket}`, and `Dict{String, ReserveORDCMarket}` for reserve markets —
i.e., the Dict-keyed pattern already exists here.

**Change:**

```julia
# Before
capacity::CapacityMarket

# After
capacity::Dict{String, CapacityMarket}   # season → market
```

Update the `MarketCollection(...)` constructor signature. `CapacityMarket` struct itself
is **not changed** — the season dimension lives entirely in this Dict wrapper.

---

## Phase 2 — Derating Calculator
*Dev B. Parallel with Phase 3. Depends on Phase 1.*

### 2a. `derating_factor_calculator.jl` — seasonal derating (both methodologies)

**File:** `src/derating_factor_updates/derating_factor_calculator.jl`  
**Module role:** Computes how much each technology type contributes to peak reliability.
Two methodologies exist — both are called from `update_simulation_derating_data!` based on
the `accreditation_methodology` case setting:

1. **`calculate_derating_data()` — TopNetLoad**: loads all-year hourly net-load CSVs
   (`load_n_vg_data_rt.csv`), ranks hours by net load, takes the top `num_top_hours ×
   simulation_years` hours, and measures each VRE/storage tech's average load reduction
   in those peak hours. Storage uses an internal dispatch simulation
   (`calculate_average_storage_cc` / `calculate_marginal_storage_cc`) that finds the
   minimum energy level needed to flatten the peak, then derives a capacity credit from
   storage duration ÷ required MWh.

2. **`calculate_derating_factors()` — PRAS ELCC/EFC**: builds a PRAS probabilistic
   reliability system over the full simulation horizon, then calls
   `PRAS.assess(base_system, augmented_system, PRAS.ELCC{ra_metric}(...), SequentialMonteCarlo)`
   to measure how much conventional firm capacity each VRE/battery tech is equivalent to.
   The `period_of_interest` currently spans all `DEFAULT_HOURS_PER_YEAR × simulation_years`
   timesteps.

The result (whichever methodology) is written to `derating_dict.csv` and later loaded by
`update_derating_factor!` dispatch functions (one per project type) which call
`set_derating!(product, scenario, value)`.

**Reference pattern for month parsing:** `src/markets_simulation/ordc_construction/ordc_construction.jl`
lines 104–126 already implements season-month parsing (`seasons`, `season_months` dict,
`month_lookup`, wrap-around handling). Copy that pattern directly.

---

#### `derating_dict.csv` format change

Currently the file has **one row** with one derating value per column (e.g.
`Thermal_CC`, `existing_Wind_zone_1`, `existing_STOR_4`). In seasonal mode it needs
**one row per season**, adding a `season` column:

```
season,   Thermal_CC,  existing_Wind_zone_1,  new_Wind_zone_1,  existing_STOR_4,  ...
summer,   0.85,        0.14,                  0.16,             0.72,             ...
winter,   0.88,        0.05,                  0.06,             0.65,             ...
```

In annual mode the file keeps its current single-row format (no `season` column).
`update_derating_factor!` reads `derating_data[1, col]` today — in seasonal mode it
iterates over rows, reading `derating_data[row, col]` for each season.

---

#### Changes for `calculate_derating_data()` — TopNetLoad

1. Accept `season_months::Dict{String, Vector{Int64}}` loaded from `capacity_seasons.csv`
   via `load_season_months`. Season names are primary keys — the derating output
   `derating_dict.csv` uses those same names in its `season` column. In annual mode
   `load_season_months` returns `Dict("annual" => collect(1:12))` (file absent fallback).

2. Parse the `month` column of `load_n_vg_data` (or derive it from the `year`/`hour`
   columns already present). For each season, build a boolean mask:
   ```julia
   season_mask = [month(row) in season_months[season] for row in eachrow(load_n_vg_data)]
   seasonal_net_load_df = load_n_vg_data[season_mask, :]
   ```

3. **VRE derating (existing and option):** apply the top-N sort and load-reduction
   calculation to `seasonal_net_load_df` instead of the full `net_load_df`. Use
   `num_top_hours` proportional to the season length (e.g. if summer is 5 months,
   use `num_top_hours × 5/12 × simulation_years`), or add a per-season
   `num_top_hours` column to `Capacity.csv`.

4. **Storage CC (existing and option):** the `calculate_average_storage_cc` /
   `calculate_marginal_storage_cc` inner functions receive `net_load_df` and `num_hours`.
   Pass the season-filtered DataFrame and its row count. The `maximum(net_load_df[:,
   "net_load"])` becomes the seasonal peak; the dispatch simulation determines the
   minimum MWh depth needed to serve that seasonal peak across season hours only.
   - Summer storage CC will reflect the shorter but intense afternoon duck-curve peaks.
   - Winter storage CC will reflect the longer extended cold-spell demand periods.

5. Write `derating_dict.csv` with the `season` column (one row per season). In annual
   mode, write as today (no `season` column, single row).

---

#### Changes for `calculate_derating_factors()` — PRAS ELCC/EFC

The PRAS API uses a `period_of_interest` argument to `make_pras_system` and `PRAS.assess`
that specifies which timesteps of the simulation horizon to include in the reliability
assessment. The key insight for seasonal accreditation: **run two separate PRAS assessments,
one per season, each restricted to that season's timesteps**.

1. Build the season hour index across all simulation years:
   ```julia
   function season_period_of_interest(season_months, simulation_years)
       hours_per_year = DEFAULT_HOURS_PER_YEAR
       indices = Int[]
       for year in 1:simulation_years
           for h in 1:hours_per_year
               # derive calendar month from hour-of-year index
               m = month_of_hour(h)   # helper: maps 1-8760 → 1-12
               if m in season_months
                   push!(indices, (year - 1) * hours_per_year + h)
               end
           end
       end
       return indices
   end
   ```
   > Note: check whether PRAS `period_of_interest` requires a contiguous `UnitRange`
   > or accepts an arbitrary `Vector{Int}`. If only contiguous ranges are supported,
   > the timeseries data may need to be restructured per season (summer hours
   > concatenated across years) before building the PRAS system. Confirm against the
   > PRAS.jl API before implementing.

2. Build `base_pras_system` and `augmented_pras_system` with the seasonal
   `period_of_interest`:
   ```julia
   pras_systems = Dict(
       season => make_pras_system(adjusted_base_system;
           period_of_interest = season_period_of_interest(season_months[season], simulation_years),
           ...)
       for season in seasons
   )
   ```

3. For each VRE type/zone and battery duration, call `PRAS.assess` once per seasonal
   base system. Store results separately:
   ```julia
   for season in seasons
       cc_result = PRAS.assess(
           base_pras_systems[season], augmented_pras_systems[season],
           PRAS.ELCC{ra_matric}(Int(ceil(capacity)), "Region"),
           PRAS.SequentialMonteCarlo(; samples = PRAS_N_SAMPLES, seed = 42))
       # write row for this season in derating_dict.csv
   end
   ```

4. Winter ELCC will naturally capture thermal generator fuel-supply risk (if outage
   timeseries reflect winter forced outage rates) and VRE intermittency in winter
   conditions. Summer ELCC will capture summer availability and duck-curve effects.

5. Write `derating_dict.csv` with `season` column (one row per season).

> **Performance note:** Running two PRAS Monte Carlo assessments per tech type doubles
> the PRAS computation cost. This is acceptable since PRAS runs are already the
> dominant runtime cost. The `PRAS_N_SAMPLES` constant can be tuned independently
> for seasonal runs if needed.

---

### 2b. `derating_factor_calculator.jl` — `update_derating_factor!` dispatch functions

**Same file, lower section (~lines 700–870).**  
**Module role of this section:** After `calculate_derating_data` or `calculate_derating_factors`
writes `derating_dict.csv`, the `update_derating_factor!` multiple-dispatch functions read
it back and call `set_derating!(product, scenario, value)` on each project. There is one
method per project type: `ThermalGenEMIS`, `HydroGenEMIS`, `RenewableGenEMIS{Existing}`,
`RenewableGenEMIS{<:BuildPhase}` (new), `BatteryEMIS{Existing}`, `BatteryEMIS{<:BuildPhase}`.
All currently read `derating_data[1, col]` — the first (only) row.

**Change (all `update_derating_factor!` methods):** Iterate over seasons:
```julia
# Before (example for Thermal/Hydro)
derating_factor = derating_data[1, get_type(get_tech(project))]
for product in get_products(project)
    set_derating!(product, scenario, derating_factor)
end

# After
for row in eachrow(derating_data)
    season = seasonal ? row["season"] : "annual"
    derating_factor = row[get_type(get_tech(project))]
    for product in get_products(project)
        set_derating!(product, scenario, season, derating_factor)
    end
end
```
In annual mode, `derating_dict.csv` has no `season` column and still has one row —
the loop runs once with `season = "annual"`, identical to current behavior.

---

### 2c. `product_creator.jl` — `Capacity` struct initialization

**File:** `src/struct_creators/simulation_structs/product_creator.jl`  
**Module role:** Reads CSV/HDF5 project input data and constructs typed `Product` objects
(`Capacity`, `Energy`, `REC`, `Reserve`, etc.) attached to each project. The `Capacity`
product is created here with initial derating values loaded from `derating_dict.csv`.

**Change:** Initialize `derating` and `accepted_perc` as nested dicts:
```julia
# Before
derating = Dict(scenario => derating_value)

# After
derating = Dict(scenario => Dict(season => derating_value for season in seasons))
```
Read `season` column from `derating_dict.csv` when present.

---

### 2d. `market_project_creator.jl` — derating extraction for CEM

**File:** `src/struct_creators/market_structs/market_project_creator.jl`  
**Module role:** Creates `MarketProject` structs from full `Project` objects for use in
`cem.jl`. Contains helper `get_project_derating(project, scenario)` which extracts the
derating factor from whichever `Capacity` product the project has (returns 0.0 if no
`Capacity` product exists). Also contains `get_project_capacity_market_bid`.

**Change:** `get_project_derating(project, scenario)` returns `Dict{String, Float64}`
(season → derating) instead of a scalar. The `update_capacity_supply_curve!` callers
(Phase 4) pass the specific season key.

---

## Phase 3 — CEM / Expected Markets
*Dev A. Parallel with Phase 2. Depends on Phase 1.*

### 3a. `market_utils.jl` — demand curve vector conversion

**File:** `src/utils/market_utils.jl`  
**Module role:** Utility functions that convert market structs into raw arrays/vectors
suitable for JuMP optimization. `make_capacity_demand_vectors` converts a
`Vector{CapacityMarket}` (one per investment period) into four parallel arrays of segment
sizes, gradients, price points, and segment counts — the form consumed by `cem.jl`.
`make_capacity_demand` does the same for the single-market actual-clearing case.

**Change to `make_capacity_demand_vectors`:**
- Input changes from `Vector{CapacityMarket}` to `Vector{Dict{String, CapacityMarket}}`
- Returns season-keyed outputs: `cap_segmentsize[season]`, `cap_segmentgrad[season]`,
  `cap_price_points[season]`, `cap_numsegments[season]` — each a `Vector` over investment periods
- `make_capacity_demand(capmarket::CapacityMarket)` used in actual clearing: **unchanged**
  (it is called once per season, receiving one `CapacityMarket` at a time)

---

### 3b. `cem.jl` — core JuMP optimization model

**File:** `src/markets_simulation/cem.jl`  
**Module role:** Implements the Capacity Expansion Model — a multi-period, multi-zone
quadratic program (QP) that simultaneously clears energy, reserve, capacity, REC, and
inertia markets to produce shadow prices used as forward-looking revenue forecasts.
`capacity_market` (line 659) is the JuMP constraint that equates derated capacity supply
to capacity demand; its dual is the capacity price returned to investors.

**Changes (all within `cem()`):**

1. Extract seasons from the capacity dict in the first investment period:
   ```julia
   seasons = collect(keys(system.inv_periods[1].capacity))
   ```

2. Replace the `d_cap` variable (current line 343):
   ```julia
   # Before
   JuMP.@variable(m, d_cap[p in invperiods, s in 1:cap_numsegments[p]] >= 0)

   # After
   JuMP.@variable(m, d_cap[p in invperiods, season in seasons,
                            s in 1:cap_numsegments[season][p]] >= 0)
   ```

3. Replace the objective capacity welfare terms (current lines 454–455):
   ```julia
   # After: sum over seasons
   + sum(d_cap[p, season, s] * cap_price_points[season][p][s] * social_npv_array[p]
         for p in invperiods, season in seasons, s in 1:cap_numsegments[season][p])
   + 1/2 * sum(cap_segmentgrad[season][p][s] * d_cap[p, season, s]^2 * social_npv_array[p]
               for p in invperiods, season in seasons, s in 1:cap_numsegments[season][p])
   ```

4. Replace the demand segment limit constraint (current line 525):
   ```julia
   JuMP.@constraint(m, [p in invperiods, season in seasons, s in 1:cap_numsegments[season][p]],
       d_cap[p, season, s] <= cap_segmentsize[season][p][s])
   ```

5. Replace the capacity balance constraint (current lines 659–660):
   ```julia
   # Before
   JuMP.@constraint(m, capacity_market[p in invperiods],
       sum(c[g, p] * max_gen[g] * derating_factor[g] for g in capacity_mkt_projects)
           == sum(d_cap[p, s] for s in 1:cap_numsegments[p]))

   # After
   JuMP.@constraint(m, capacity_market[p in invperiods, season in seasons],
       sum(c[g, p] * max_gen[g] * derating_factor[g][season] for g in capacity_mkt_projects)
           == sum(d_cap[p, season, s] for s in 1:cap_numsegments[season][p]))
   ```
   Note: `c[g, p]` (commitment decision) stays **one variable per project-period** across
   all seasons. A project is either committed to all seasonal obligations or none. The
   season-specific effective capacity contribution comes entirely from `derating_factor[g][season]`.

6. Output `capacity_price` (current line 690):
   ```julia
   # Before: AxisArray indexed by invperiods
   capacity_price = JuMP.dual.(capacity_market) ./ OBJ_SCALE

   # After: Dict{String, AxisArray} — one price vector per season
   capacity_price = Dict(season =>
       AxisArrays.AxisArray(JuMP.dual.(capacity_market)[:, season] ./ OBJ_SCALE, invperiods)
       for season in seasons)
   ```

`capacity_accepted_perc[g][p]` is **unchanged** — it is still per project-period (same
commitment fraction for all seasons).

---

### 3c. `cem_creator.jl` — CEM problem construction from CSV inputs

**File:** `src/struct_creators/market_structs/cem_creator.jl`  
**Module role:** Reads all CSV and HDF5 inputs for one investor for one iteration year and
assembles the `MarketClearingProblem` struct that `cem.jl` consumes. Creates `CapacityMarket`,
`EnergyMarket`, `ReserveUpMarket`, etc. structs and bundles them into `MarketCollection`
per investment period.

**Changes (inside `create_cem_mkt_clr_problem`):**

1. Load season definitions from `capacity_seasons.csv` (primary key: `name`):
   ```julia
   seasons_file = joinpath(sys_data_dir, "markets_data", "capacity_seasons.csv")
   season_months = load_season_months(seasons_file)  # Dict{String, Vector{Int}}
   seasons = collect(keys(season_months))            # e.g. ["summer", "winter"]
   ```
   `load_season_months` is a new utility function in `src/utils/general.jl` (or
   `market_utils.jl`) that reads the CSV and applies `month_lookup` with wrap-around
   handling. When the file is absent, returns `Dict("annual" => collect(1:12))`.

2. Read the full `Capacity.csv` (all rows, not `[1, :]`) and index by `season`:
   ```julia
   capacity_mkt_params = read_data(capacity_mkt_param_file)
   seasonal = get_seasonal_capacity_market(get_case(simulation))
   # rows keyed by season name (foreign key into capacity_seasons.csv)
   cap_params_by_season = Dict(row["season"] => row
                               for row in eachrow(capacity_mkt_params))
   ```

3. For each investment period `p`, build a `Dict{String, CapacityMarket}`:
   ```julia
   capacity_markets[p] = Dict(
       season => create_capacity_demand_curve(
           cap_params_by_season[season],
           seasonal_peak_load(energy_markets[p], season_months[season]),
           irm_scalar, delta_irm, capacity_mkt_active
       )
       for season in seasons
   )
   ```

4. Seasonal peak load is derived locally from the already-loaded `EnergyMarket` demand
   arrays by filtering to season months — no new struct field on `AgentSimulation`:
   ```julia
   function seasonal_peak_load(energy_market, month_indices)
       # filter rep-hour timestamps to season months, take max zonal load
   end
   ```

---

### 3d. `capacity_profit.jl` — expected capacity revenue for NPV

**File:** `src/investor_functions/prediction/capacity_profit.jl`  
**Module role:** Called during each investor's NPV computation. Uses CEM-predicted
`capacity_price` and the project's `accepted_perc` and `derating` to compute expected
capacity revenues per year. Revenue feeds directly into the project's financial decision
(build / don't build).

**Change to `update_capacity_revenues!` for `Capacity`:**
```julia
# Before (single annual market)
capacity_revenues += capacity_prices[scenario_name][years] .*
                     get_accepted_perc(product)[scenario_name][years] *
                     max_cap *
                     get_derating(product)[scenario_name]

# After (sum over seasons)
for season in keys(capacity_prices[scenario_name])
    capacity_revenues += capacity_prices[scenario_name][season][years] .*
                         get_accepted_perc(product, scenario_name, season)[years] *
                         max_cap *
                         get_derating(product, scenario_name, season)
end
```

Signature: `capacity_prices` argument type changes from
`Dict{String, AxisArray{Float64,1}}` to `Dict{String, Dict{String, AxisArray{Float64,1}}}`.

---

### 3e. `save_load_utils.jl` — HDF5 serialization of expected market data

**File:** `src/utils/save_load_utils.jl`  
**Module role (expected market data section):** `save_expected_market_data` and
`load_expected_market_data` (around lines 1929 and 2058) persist and restore the full
output of `cem()` to/from `{investor_dir}/expected_market_data/{scenario}_year_{year}.h5`.
`capacity_price` is currently saved as a flat AxisArray group.

**Change:** Save `capacity_price` as nested groups, one per season:
```julia
# save_expected_market_data
cap_g = create_group(f, "capacity_price")
for (season, prices) in capacity_price
    save_axisarray!(create_group(cap_g, season), prices)
end

# load_expected_market_data
cap_g = f["capacity_price"]
capacity_price = Dict(season => load_axisarray(cap_g[season]) for season in keys(cap_g))
```

---

## Phase 4 — Realized Market Clearing
*Dev B. Depends on Phase 1 and Phase 2.*

### 4a. `actual_capacity_mkt_clearing.jl` — supply curve and JuMP clearing model

**File:** `src/markets_simulation/actual_capacity_mkt_clearing.jl`  
**Module role:** Two responsibilities: (1) `update_capacity_supply_curve!` — builds the
supply curve by pushing each capacity-eligible project's `[name, max_cap, bid, derating]`
tuple into a `Vector`; (2) `capacity_market_clearing` — runs a small JuMP social-welfare
maximization that equates derated supply to piecewise-linear demand and returns the shadow
price (capacity price) and per-project accepted bid fractions.

**Changes:**

1. `update_capacity_supply_curve!` — add `season::String` argument, use seasonal derating:
   ```julia
   function update_capacity_supply_curve!(supply_curve, product::Capacity, project, scenario, season)
       push!(supply_curve, [get_name(project),
                            get_maxcap(project),
                            get_project_capacity_market_bid(project),
                            get_project_derating(project, scenario)[season]])  # seasonal derating
   end
   ```

2. `capacity_market_clearing` — **no structural change**; it already operates on a generic
   supply curve and a single `CapacityMarket`. It is called once per season by the caller.

---

### 4b. `actual_market_simulation.jl` — realized market orchestration

**File:** `src/markets_simulation/actual_market_simulation.jl`  
**Module role:** Top-level function that runs the full actual market simulation for one
iteration year: energy dispatch (ED/UC/MD), reserve clearing, capacity clearing, REC
clearing. The capacity section (~lines 175–260) currently: reads `Capacity.csv`, builds
one supply curve, creates one demand curve, clears the market once, stores the single price
and accepted bids in `MarketPrices`.

**Changes (capacity section only):**

```julia
# Load canonical season definitions (primary key: name → months)
seasons_file = joinpath(simulation_dir, "markets_data", "capacity_seasons.csv")
season_months = load_season_months(seasons_file)  # Dict{String, Vector{Int}}
seasons = collect(keys(season_months))

# Load per-season market params (season column is FK into capacity_seasons.csv)
capacity_mkt_params_all = read_data(capacity_mkt_param_file)
cap_params_by_season = Dict(row["season"] => row for row in eachrow(capacity_mkt_params_all))

capacity_price_dict = Dict{String, AxisArrays.AxisArray}()
capacity_accepted_bids_dict = Dict{String, Dict{String, Float64}}()

for season in seasons
    season_month_indices = season_months[season]

    # Seasonal peak load: filter timeseries to season months
    seasonal_peak = maximum(peak_load_series[m] for m in season_month_indices)

    # Build supply curve for this season
    seasonal_supply_curve = Vector{Union{String, Float64}}[]
    for project in capacity_market_projects
        for product in get_products(project)
            seasonal_supply_curve = update_capacity_supply_curve!(
                seasonal_supply_curve, product, project, pcm_scenario, season)
        end
    end
    sort!(seasonal_supply_curve; by = x -> x[3])

    seasonal_demand_curve = create_capacity_demand_curve(
        cap_params_by_season[season], seasonal_peak, irm_scalar, delta_irm, capacity_active_bool)

    seasonal_price, seasonal_bids = capacity_market_clearing(
        seasonal_demand_curve, seasonal_supply_curve, solver)

    capacity_price_dict[season] = seasonal_price
    capacity_accepted_bids_dict[season] = seasonal_bids
end

set_capacity_price!(market_prices, "realized", capacity_price_dict)
```

---

### 4c. `MarketPrices.jl` — realized price storage struct

**File:** `src/structs/MarketPrices.jl`  
**Module role:** Accumulates all realized market clearing prices during a simulation year.
Uses string-keyed Dicts so multiple prices can be stored under keys like `"realized-ed"`,
`"realized-uc"`, `"realized-md"`. The capacity price is currently stored under key
`"realized"` as a single `AxisArray`.

**Change:** Capacity price stored as `Dict{String, AxisArray}` (season → price) under
key `"realized"`, consistent with how energy stores multiple resolution prices. The
`set_capacity_price!` / `get_prices` accessors already accept a Dict — verify or update
their type annotations accordingly.

---

### 4d. `realized_profits_calculator.jl` — actual profit computation

**File:** `src/investor_functions/realized_profits_calculator.jl`  
**Module role:** After market clearing, computes the actual monetary profit for each
project from each product for the relevant forward year. For `Capacity`, the profit is
`size × derating × cleared_price × accepted_bid_fraction`, mapped to year
`iteration_year + capacity_forward_years - 1`.

**Change to `calculate_realized_profit` for `Capacity`:**
```julia
# Before
profit = size * get_derating(product)[pcm_scenario] *
         get_prices(market_prices, product)["realized"][1] *
         capacity_accepted_bids[project_name]

# After: sum over seasons
profit = 0.0
capacity_prices = get_prices(market_prices, product)["realized"]  # Dict{String, AxisArray}
for (season, season_price) in capacity_prices
    if in(project_name, keys(capacity_accepted_bids[season]))
        profit += size *
                  get_derating(product, pcm_scenario, season) *
                  season_price[1] *
                  capacity_accepted_bids[season][project_name]
    end
end
```

---

### 4e. `save_load_utils.jl` — HDF5 serialization of realized market data

**File:** `src/utils/save_load_utils.jl`  
**Module role (realized market data section):** `_save_optional_axis_dict!` (around
line 836) saves the `capacity_price` field of `MarketPrices`. With the type change to
`Dict{String, AxisArray}`, add a second level of nesting (season sub-groups) using the
existing `_save_optional_axis_dict!` helper recursively.

---

## Phase 5 — Integration & Testing

**5a. Backward compatibility test** — set `seasonal_capacity_market = false`. Run the
full existing test suite. Zero output differences expected; all capacity market paths
follow the `["annual"]` branch with single-row `Capacity.csv` files unchanged.

**5b. Seasonal mode functional test** — set `seasonal_capacity_market = true`, create a
two-row `Capacity.csv` with distinct IRM and CONE values for summer and winter.
Verify:
- CEM produces two capacity prices per investment year (summer, winter).
- Solar resources have higher derating in summer (summer top-N hours favour solar-heavy periods).
- Thermal resources have similar derating across seasons; storage may differ.
- Annual revenue = summer revenue + winter revenue (check against manual calculation).

**5c. CEM solver test** — confirm `capacity_market[p, season]` constraints solve to
optimality with CPLEX. Check duals are non-negative and dimensionally consistent.

**5d. HDF5 round-trip test** — call `save_expected_market_data` → `load_expected_market_data`
and `save_realized_market_prices` → load. Verify seasonal capacity prices survive the
round-trip with the correct season keys and values.

---

## Complete File Reference

| File | Module role | What changes |
|------|-------------|--------------|
| `src/structs/CaseDefinition.jl` | Simulation-wide settings and toggles | Add `seasonal_capacity_market::Bool` field |
| `src/structs/products/Capacity.jl` | Per-project capacity market participation data | `derating` and `accepted_perc` gain season nesting; all accessors updated |
| `src/structs/market_structs/CapacityMarket.jl` | Piecewise linear capacity demand curve definition | **Unchanged** |
| `src/structs/market_structs/MarketCollection.jl` | Per-investment-period bundle of all market structs | `capacity` field type: `CapacityMarket` → `Dict{String, CapacityMarket}` |
| `src/structs/market_structs/MarketProject.jl` | Optimization-ready project representation for CEM | `derating_factor` type: `Float64` → `Dict{String, Float64}` |
| `src/structs/MarketPrices.jl` | Accumulates realized clearing prices during simulation | `capacity_price` storage: `AxisArray` → `Dict{String, AxisArray}` |
| `src/markets_simulation/cem.jl` | JuMP multi-period capacity expansion / price prediction model | `d_cap`, `capacity_market` constraint, objective, `capacity_price` output all gain season dimension |
| `src/markets_simulation/actual_market_simulation.jl` | Orchestrates actual (realized) market simulation | Capacity section loops over seasons; collects per-season prices and accepted bids |
| `src/markets_simulation/actual_capacity_mkt_clearing.jl` | Supply curve construction + JuMP actual capacity clearing | `update_capacity_supply_curve!` adds `season` arg; `capacity_market_clearing` unchanged |
| `src/markets_simulation/ordc_construction/ordc_construction.jl` | ORDC timeseries construction | **Reference only** — season/month parsing pattern to copy |
| `src/struct_creators/market_structs/cem_creator.jl` | Builds `MarketClearingProblem` from CSVs for CEM | Reads all `Capacity.csv` rows; builds `Dict{String, CapacityMarket}` per period; derives seasonal peak |
| `src/struct_creators/market_structs/market_project_creator.jl` | Creates `MarketProject` from `Project`; derating extraction | `get_project_derating` return type: `Float64` → `Dict{String, Float64}` |
| `src/struct_creators/simulation_structs/product_creator.jl` | Creates typed `Product` objects from input data | `Capacity` init uses nested dicts; reads `season` column from `derating_dict.csv` |
| `src/derating_factor_updates/derating_factor_calculator.jl` | Top-N net-load derating + PRAS ELCC/EFC + `update_derating_factor!` dispatch | **TopNetLoad:** filter hourly data to season months, run top-N selection and storage CC dispatch per season. **PRAS:** build seasonal `period_of_interest` index, call `PRAS.assess` separately per season. All paths write `derating_dict.csv` with a `season` row. `update_derating_factor!` methods iterate season rows when seasonal. |
| `src/investor_functions/prediction/capacity_profit.jl` | Expected capacity revenue for investor NPV | `update_capacity_revenues!` sums over seasons |
| `src/investor_functions/realized_profits_calculator.jl` | Post-clearing realized profit per product | `calculate_realized_profit` for `Capacity` sums over seasons |
| `src/utils/market_utils.jl` | Converts market structs to JuMP-ready arrays | `make_capacity_demand_vectors` accepts and returns season-keyed structures |
| `src/utils/general.jl` | General utilities including `month_lookup` | Add `load_season_months(path)` — reads `capacity_seasons.csv`, applies `month_lookup` with wrap-around, returns `Dict{String, Vector{Int}}`; returns `Dict("annual" => collect(1:12))` when file absent |
| `src/utils/save_load_utils.jl` | HDF5 serialize/deserialize for all simulation state | `capacity_price` in expected + realized data gains season sub-groups; `CaseDefinition` gets `seasonal_capacity_market` field |
| `capacity_seasons.csv` *(new input file)* | Canonical season definition | `name` (PK) → `months` range. Read by `load_season_months`. Referenced as FK by `Capacity.csv` and `derating_dict.csv`. |

---

## Key Design Decisions

- **Explicit toggle in `CaseDefinition`** (`seasonal_capacity_market::Bool`), not inferred
  from file structure. Mirrors `static_capacity_market::Bool` exactly.
- **`capacity_seasons.csv` is the single source of truth** for which seasons exist and which
  months each covers. Season `name` is the primary key referenced by `Capacity.csv` (market
  params), `derating_dict.csv` (derating values), and all in-memory data structures. No
  hardcoded `"summer"` or `"winter"` strings anywhere in code.
- **`Capacity.csv` format is mode-specific.** Annual mode keeps the existing single-row format
  with no `season` column — no file changes needed for existing cases. Seasonal mode adds a
  `season` column (FK into `capacity_seasons.csv`) with one row per season; `season_months`
  is **not** in `Capacity.csv` (it lives only in `capacity_seasons.csv`).
- **Adding a new season** requires only a new row in `capacity_seasons.csv`, a matching row
  in `Capacity.csv`, and a matching row in `derating_dict.csv`. No code changes.
- **One `c[g,p]` commitment variable across all seasons.** A project is in or out of all
  seasonal obligations simultaneously. Season-specificity comes only from seasonal derating
  factors. This simplification can be relaxed later to `c[g,p,season]` if season-specific
  opt-out is needed.
- **Seasonal peak load derived at runtime** from the already-loaded load timeseries filtered
  to `season_months` — no new fields on `AgentSimulation` or `CaseDefinition`.
- **`CapacityMarket` struct unchanged.** The season dimension lives in the `Dict` wrapper
  at `MarketCollection.capacity`, consistent with how reserves are already structured.

---

## Work Division

| Phase | Dev A | Dev B |
|-------|-------|-------|
| Phase 1 — Structs + CSV format | joint | joint |
| Phase 2 — Derating calculator + product/project creators | | ✓ |
| Phase 3 — CEM, expected market simulation, revenue, HDF5 | ✓ | |
| Phase 4 — Realized clearing, MarketPrices, realized profits, HDF5 | | ✓ |
| Phase 5 — Testing | ✓ | ✓ |

**Dependencies between streams:**
- Phase 2 and Phase 3 can start simultaneously once Phase 1 is merged.
- Phase 4 depends on Phase 2 (`derating_factor` type from `get_project_derating`).
- Both streams touch `save_load_utils.jl` in different sections — coordinate to avoid merge conflicts.
