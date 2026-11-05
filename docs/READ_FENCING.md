# Log-free quorum reads

The default `read_mode` is `quorum`; `-read-mode=barrier` retains the original
replicated barrier path for controlled comparison and compatibility diagnostics.
Both modes preserve linearizable leader-only reads. A follower still returns the
structured leader hint; quorum mode does not serve follower-local/stale reads.

Each quorum read checks that this leader has committed an entry in its current
term, captures the committed keyspace-command target, asks the adopted Raft
VerifyLeader API for fresh quorum confirmation, waits for completed FSM application
through that target, copies the value and checks that leader term/state did not
change. Failure or cancellation returns no value. A new leader waits within the read deadline until its current-term entry is
committed; it does not return an unfenced value during initialization. The application does
not infer leadership from a timer or skip quorum verification under load.

HashiCorp Raft 1.7.1 AppliedIndex measures dispatch to its asynchronous FSM, not
completion. It is therefore not used as the completion oracle. This adapter
tracks successful mutation application and deterministic capacity no-ops using
a notification channel; completed-application waits do not poll. The adopted library exposes no commit
notification for its internal leader no-op, so only the brief current-term
initialization wait probes commitment every 5 ms. This wait is canceled by the
request deadline and rechecks term/state; the steady path does not poll. Snapshot restore publishes completion
only after store replacement succeeds. Raft configuration, no-op and barrier
entries consume log indices without changing keyspace, so the adapter resolves
a committed index to the latest relevant command, using compacted snapshot
metadata for already-completed prefixes. A small index/term/target cache avoids
repeating log traversal on unchanged read-only load; no values are cached.

A context bounds the caller wait. Shared admission remains occupied until any
submitted verification/barrier future actually resolves, including after caller
cancellation. This prevents canceled requests from creating unaccounted work.
There is no transparent fallback from a failed quorum check to an unsafe read.
The barrier timeout is checked positive before submission.

References checked against the locally pinned module and primary sources:
[HashiCorp Raft 1.7.1 API](https://github.com/hashicorp/raft/blob/v1.7.1/api.go),
[FSM dispatch/completion](https://github.com/hashicorp/raft/blob/v1.7.1/fsm.go),
[Raft paper, section 8](https://raft.github.io/raft.pdf).

This document describes the implementation; recorded check manifests determine
its verified scope. This is not an independent proof, production capacity claim,
new membership protocol or mixed-version upgrade qualification. Performance
comparison, complete failure-matrix verification and exact-candidate qualification
must accompany the implementation before closing P5.

## Local checkpoint

See the retained local read-fence checkpoint (not distributed with source)
and its CHECKPOINT_REPORT.md for the source-linked composite evidence and local
paired measurement. Prior failed invocations are preserved explicitly. Probe
lifecycle remains leader-only and quorum-bound; replacing it with cheap local
startup/liveness/readiness semantics is a separate P6 task, not claimed here.
