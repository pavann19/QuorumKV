# Verification status

Reviewed on 2026-11-01 against candidate `18c5b32c271a7665ea7bb361198ad113382d1570` in [pavann19/QuorumKV](https://github.com/pavann19/QuorumKV). Proposed in [PR #3](https://github.com/pavann19/QuorumKV/pull/3); this is a dated snapshot, not a statement that it has merged.

## Hosted baseline evidence

| Workflow | Result | Source revision |
|---|---|---|
| [CI](https://github.com/pavann19/QuorumKV/actions/runs/41329079151) | PASS | `18c5b32c271a` |
| [Diagnostics and checkpoint regressions](https://github.com/pavann19/QuorumKV/actions/runs/41329079136) | PASS | `18c5b32c271a` |

Follow each run for exact jobs, logs and artifacts. These checks apply to the listed source baseline. For later documentation or runtime commits, inspect their own PR checks and `main` workflow runs. Baseline evidence does not automatically certify a later revision.

## Outstanding operational evidence

The retained soak failed after approximately 8 hours 5 minutes on Windows checkpoint publication (WinError 5). The reviewed revision contains a repair and regression coverage, but the original run remains FAIL. No replacement 24-hour PASS is established. Local process faults and bounded checker histories do not prove multi-host availability or physical power-loss durability.

## Safe continuation

During main-promotion checks, the Linux diagnostics PR run [41334221220](https://github.com/pavann19/QuorumKV/actions/runs/41334221220) failed the fixed lost-reply experiment during worker crash recovery. The matching push job passed. The worker previously discarded the subprocess output, so that receipt does not establish the underlying cause. The fixed synthetic regression now retains its output in operator/CI logs; that diagnostic change alone is not a root-cause fix. Preserve this failed run when evaluating subsequent checks.

1. Reproduce the relevant focused regression in an isolated environment before changing behavior.
2. Preserve failing artifacts and record the new source revision, configuration, workload and outcome separately.
3. Keep optional AI output advisory; it cannot change admission policy, replay financial writes or operate cluster membership autonomously.
4. For runtime changes, rerun relevant tests and CI before making release claims.

No fresh long-running, paid-cloud or production validation was performed for this documentation update. See the [documentation index](README.md) for setup, tests and recovery.
