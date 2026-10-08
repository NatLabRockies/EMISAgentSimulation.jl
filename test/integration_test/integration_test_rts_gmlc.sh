#!/bin/bash
# RTS-GMLC 3-zone integration test: initialize the committed example spec, validate it, stamp a case,
# run 2 simulated years, then check the results with test/integration_test/check_run.jl.
#
# The run takes 1-2 hours, so submit it as a batch job from the repository root, e.g. with SLURM:
#   sbatch --job-name=rts_gmlc_2yr --account=<account> --partition=<partition> --nodes=1 \
#          --time=04:00:00 --output=rts_gmlc_2yr_%j.out test/integration_test/integration_test_rts_gmlc.sh
# or add scheduler directives for your HPC (#SBATCH, #PBS, ...) directly below this header.
#
# To check any finished run without running anything:
#   julia --project=. test/integration_test/check_run.jl --results-dir <path to Results/<run name>>

set -eo pipefail

# ---- User settings (override by exporting the variable before submitting) -----------------------

# Repository root. Default: the directory the job was submitted from, else the directory above this script.
REPO=${EMIS_REPO:-${SLURM_SUBMIT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}}

# Project spec to initialize. Default: the committed RTS-GMLC example; change only to test another spec.
SPEC_DIR=${EMIS_RTS_SPEC_DIR:-${REPO}/config/example_systems/rts_gmlc/project_spec}

# Directory for Sienna temporary time-series files. Default: /scratch/$USER; use a fast local filesystem.
SCRATCH_DIR=${EMIS_SCRATCH_DIR:-/scratch/${USER}}

# Where the project, case and results are written (recreated on every run, several GB). Default: under the scratch directory.
OUTPUT_ROOT=${EMIS_RTS_OUTPUT_ROOT:-${SCRATCH_DIR}/emis_rts_gmlc_integration}

# Case folder name. Change it to keep several cases side by side in different output roots.
CASE_NAME=${CASE_NAME:-rts_gmlc_2yr}

# Result folder name inside the case. Default: case name plus the job ID (or a timestamp outside a scheduler).
RUN_NAME=${RUN_NAME:-${CASE_NAME}_${SLURM_JOB_ID:-$(date +%Y%m%d_%H%M)}}

# Extra arguments for run_EMIS_template.jl, e.g. "--hpc" or "--pras-threads 8".
RUN_ARGS=${RUN_ARGS:-}

# ---- Environment setup: add the commands your system needs here ---------------------------------
# For example: module loads (Julia, solver), JULIA_DEPOT_PATH, solver license variables.



# --------------------------------------------------------------------------------------------------

cd "${REPO}"
julia --project="${REPO}" -e 'import Pkg; Pkg.instantiate()'

setup_log=$(julia --project="${REPO}" test/integration_test/setup_case.jl --spec-dir "${SPEC_DIR}" \
    --output-root "${OUTPUT_ROOT}" --scratch-dir "${SCRATCH_DIR}" --case-name "${CASE_NAME}" --force | tee /dev/stderr)
CASE_DIR=$(echo "${setup_log}" | sed -n 's/^CASE_DIR=//p' | tail -1)
[[ -n "${CASE_DIR}" && -f "${CASE_DIR}/run_EMIS_template.jl" ]] || { echo "Case setup failed" >&2; exit 1; }

echo "Running ${CASE_DIR}/run_EMIS_template.jl as run ${RUN_NAME} (started $(date))"
start=$(date +%s)
# shellcheck disable=SC2086
julia --project="${REPO}" --threads=auto "${CASE_DIR}/run_EMIS_template.jl" --run_name "${RUN_NAME}" ${RUN_ARGS}
end=$(date +%s)
echo "Run finished $(date); elapsed $(( (end - start) / 60 )) minutes"

julia --project="${REPO}" test/integration_test/check_run.jl \
    --results-dir "${CASE_DIR}/Results/${RUN_NAME}" --elapsed-seconds "$(( end - start ))"
