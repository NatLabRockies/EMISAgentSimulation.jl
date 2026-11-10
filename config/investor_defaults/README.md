# Runtime Investor Defaults

These files are package runtime defaults, not the user project template. The
initializer copies the shared `markets_data/` files into the generated case and
then applies the matching investor-specific files under `investors/`.

The shared fixture includes calibrated market inputs that are common across the
reference investors, including energy, capacity, REC, carbon, reserve, and
resource-adequacy inputs. Investor-specific differences are kept under the
matching investor directory, including calibrated energy and reserve inputs and
`investor_belief.csv`.

Inputs are applied in this order, with later sources taking precedence over
earlier sources when they provide the same file:

1. Package defaults in this directory.
2. A configured `reference_case_dir`.
3. User-provided files in `project_spec/markets_data/` and
   `project_spec/investors/<investor>/`.

Therefore, user-provided files are preferred, followed by reference-case files,
with these package defaults serving as the final fallback when neither source
provides an input.

The editable example for users is
`config/project_templates/project_spec/`. Add project-specific overrides there
rather than modifying these package defaults.
