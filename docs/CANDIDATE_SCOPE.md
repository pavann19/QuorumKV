# Case study candidate scope

This candidate includes the supported KV service: WAL/snapshot/command profiles
0/1, authenticated client/peer boundaries, quorum read fencing, bounded requests,
conservative retries and copy-based format migration. Default replication groups
remain one entry because the optional larger-group candidate failed its read-tail
gate. No production, sustained capacity, power-loss or endurance claim is made.

Receipt-aware operation identity, journal, command and snapshot experiments are
preserved in the original development workspace and excluded from this candidate.
No public method advertises exactly-once processing or safe uncertain-write replay.
Only the isolated copy was curated; original source and evidence remain intact.

The separate diagnostics worker and its checkpoint regressions remain included
with their existing offline/authority boundaries. No model quality is claimed.
Raw runs, runtime databases, PKI/private keys, local continuation packs and source
archives are excluded from the source distribution. Public source still needs
review and explicit authorization before any push or release.
