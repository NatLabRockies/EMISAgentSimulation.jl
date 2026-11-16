# Working on EMISAgentSimulation.jl with Claude Code

Shared rules for Claude Code sessions in this repo. Verified domain facts live in the skills under `.claude/skills/`;
this file covers how to work.

## Verified knowledge: `.claude/skills/`

- `emis`: project initialization, what scenario and investor files control, which code depends on unit sizes, and
  running tests and jobs on Kestrel.
- `sienna-psy`: how PowerSystems/PowerSimulations systems are read, modified and used.
- `sienna-pras`: PRAS conversion and derating.

Read the matching skill before you touch PSY/PSI, PRAS/derating or EMIS data or code.

When you find an issue in those areas, add it to the matching skill in the same turn:
- Each entry has the claim, its evidence (`file:line`, a data check or command output) and the date.
- Label unconfirmed explanations *hypothesis*.
- If an entry turns out wrong, correct it explicitly. Don't delete it silently.
- Store only what you verified. Don't store recollections or study results.
- Skills pin a commit. Re-grep line numbers before citing them.
- Tag facts that only hold on one branch with that branch.

## Code search: graph first, then grep

Use the graphify knowledge graph to get oriented when you look up a function, debug, trace a code path or verify a
claim about the code.

- Build it once with `/graphify .` from the repo root. This needs the graphify skill (`uv tool install graphifyy`). The
  output goes to `graphify-out/`, which git ignores.
- Query it with `graphify query "<question>"`, `graphify explain "<symbol>"` or `graphify path "A" "B"`, run from the
  repo root.
- **Before stating callers or line numbers, confirm with grep or by reading the file.** The Julia extractor resolves
  calls only within one file, so calls between files are missing. For example, the graph cannot show
  `initialize_emis_project` → `write_system_inputs`. The graph is good for orientation: files, structs, and how doc and
  diagram concepts connect to code.
- After a branch switch or code edits, run `/graphify . --update` before trusting the graph. Otherwise say it may be
  stale.

## Changes must not silently change results

This is production simulation code. A silent behavior change can invalidate a multi-day HPC run without anyone
noticing.

- When a task is framed as "just move where X is stored" or "just refactor Y", audit the actual diff hunk by hunk.
  Look for reordering, changed formulas or conditions, added guards, and dropped checks.
- If a behavior change is bundled in because it seems necessary or helpful, call it out separately from the start: say
  which part is purely mechanical and which is not, and what the latter does differently.
- When asked to re-verify, re-derive instead of re-asserting:
  - Re-read the diffs fresh.
  - Check whether a reader and its paired writer now point at different places.
  - Check nearby code the change didn't touch but could affect, such as checkpoint save/load next to a path change.
- If keeping a bundled change conflicts with "results must not change", prefer reverting it.

## Prefer reusing the default code path

Before writing a separate implementation for a special case, check whether the existing default path can produce the
same effect with different inputs. For example, a single-node PRAS system is the normal zonal path with every line
capacity set very large (a "copper plate"), not a custom topology-collapsing function. Raise this option even when
nobody asks.

## HPC runs: check live state before reporting

Before saying a job or step is still pending, check what actually happened:
- `squeue -u $USER`
- `sacct -S today`
- the newest files in the run's output folder

`sacct` can show `COMPLETED 0:0` when Julia failed inside a batch or torc job, so read the job log. Report failures
before repeating next steps.
