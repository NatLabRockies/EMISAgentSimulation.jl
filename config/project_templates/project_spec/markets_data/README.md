# Optional Market Overrides

Place project-specific market input overrides here when the package runtime
defaults in `config/investor_defaults/` or a `reference_case_dir` are not
sufficient for the study. This directory is part of the editable project
template; it is not the source of the package runtime defaults.

Common override files include:

- `annual_growth.csv`
- `Energy.csv`
- `Capacity.csv`
- `REC.csv`, `REC_High_RPS.csv`, `REC_Mid_RPS.csv`, `REC_Low_RPS.csv`
- `CarbonTax.csv`
- `reserve_products.csv`
- `reserve_up_mkt_param.csv`
- `reserve_down_mkt_param.csv`
- `{High,Mid,Low}_reserve_penalty/*.csv`
- `derating_data/{scenario}/derating_dict.csv`
- `resource_adequacy_targets.csv`
- `symmetric_belief.csv`
- `symmetric_scenario_data.csv`
- `symmetric_scenario_multiplier_data.csv`

The initializer applies inputs in this order: package defaults, reference-case
files, then files supplied here. Files under
`../investors/{investor}/markets_data/` can override the shared defaults for one
investor when calibrated agent-specific values are needed.

Do not put raw system time series here. User-supplied raw profiles belong under the path referenced by `time_series_data_dir` in `project_spec.csv`, usually `timeseries/`.
