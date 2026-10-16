# Timeseries Input Template

This directory is the canonical timeseries root. Copy it or generate an equivalent
layout with your own preprocessing workflow.

The CSV files are one-row sample templates. Replace the sample value columns and rows
with the exact zone, generator, and reserve-service names from your system. Provide
hourly, preprocessed data for every scenario and simulation year listed in
`../scenarios.csv`.

```text
{scenario}/sim_year_{n}/
├── Load/
│   ├── DAY_AHEAD_regional_Load.csv
│   └── REAL_TIME_regional_Load.csv
├── WIND/
│   ├── DAY_AHEAD_wind.csv
│   └── REAL_TIME_wind.csv
├── PV/
│   ├── DAY_AHEAD_pv.csv
│   └── REAL_TIME_pv.csv
└── Reserves/
    ├── DAY_AHEAD_regional_{product}.csv
    └── REAL_TIME_regional_{product}.csv
```

Required format:

- Load, wind, and PV files have `Year,Month,Day,Period` headers followed by value columns.
- Reserve files have `Year,Month,Day,1,...,24` headers; each row contains one day's 24
    hourly reserve values.
- Load value column headers are `zones.csv` `zone_id` values: column `N` holds the load of
    zone `N`, i.e. of the PSY area in that row's `area_name`.
- In the standard workflow, you run `initialize_emis_project` and then create a case with
    `new_emis_case`; you do not need to run `create_sys_with_timeseries`. Wind and PV value
    column headers must exactly match the corresponding PSY generator names. The standard
    workflow does not provide a column-mapping option, so rename columns during
    preprocessing if needed.
- A reserve file is associated with a PSY service by the product name in its filename
    (case-insensitively). You do not provide a service-to-column mapping in the standard
    workflow; reserve values use the 24 hour-of-day columns described above.
- Use finite numeric values at hourly resolution. In the standard project workflow, load,
    wind, PV, and reserve values are all supplied in MW. During system construction, EMIS
    converts wind/PV output to fractions of generator rating and reserve values to
    fractions of the service requirement, as required by PSY's scaling factors. Load is
    scaled to the configured system peak.
- The builder wraps profiles cyclically when a forecast window crosses the end of the supplied profile.

The actual data values, number of rows, scenario directories, and reserve-product files are system-specific and must be supplied by the user.
