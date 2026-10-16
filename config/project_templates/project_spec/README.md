# Project Spec Starter

This directory is the canonical minimal project-init example used by the unit
tests. It contains user-owned project metadata, system configuration, outage
input, and minimal canonical time series. Runtime defaults supply investor
characteristics and market inputs wherever the project does not provide them.

The `system_filepath` row in `project_spec.csv` gives the path to the PSY system JSON;
relative paths are resolved from this folder. The `system.json` value here is a
placeholder used by the automated project-initialization tests, not a usable system.
For your project, replace it with the path to a real PSY system JSON and provide the
matching `<system_stem>_time_series_storage.h5` sidecar.

The optional `markets_data/` directory contains guidance for adding
project-specific overrides. The package defaults under
`config/investor_defaults/` populate the generated project when investor inputs
are omitted.

This folder is the copyable starter input bundle for project initialization. Copy the
whole folder, keep the filenames unchanged, and replace the sample values with data for
your system.

For the full workflow, see `src/project_init/README.md`.

Useful details in this starter folder:

- `timeseries/README.md`: required time series folder and CSV layout
- `markets_data/README.md`: optional study-specific market override files
- `projectoptions.csv`: minimal new-build candidate schema
- `projectexisting.csv`: required existing-project ownership input
- `devices_to_remove.csv`: required device-removal input, which may contain only
	the header when no devices should be removed

The sample files here are format examples, not a complete runnable system. Add or point
to your real PSY system JSON, matching HDF5 sidecar, outage data, and full time series
before running a real project initialization.
