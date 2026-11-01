# Architecture

The replicated request path is client RPC, identity/request checks, leader KV service, HashiCorp Raft quorum commitment, and committed FSM application to the custom WAL-backed store. Raft log/stable state is stored separately using BoltDB. A replicated read commits a quorum barrier and waits for local application before accessing the store.

HashiCorp Raft supplies elections, quorum commitment and snapshot hooks; Jille supplies the Raft gRPC transport. QuorumKV supplies identity policy, request limits, state-machine storage, service semantics and the process/fault harness. See the [ownership boundary](RAFT_BOUNDARY.md).

A timeout or cancelled RPC can leave a write's commit outcome unknown; it is not a rollback guarantee. Copying the application WAL alone is not a whole-node backup. An application I/O failure during committed application stops the process rather than silently acknowledging a divergent replica.

Optional mTLS uses operator-provisioned trust and identity allowlists. Plaintext traffic is restricted to literal loopback. Membership is static in this scope. Certificate lifecycle and cluster operations are not delegated to an AI model.

The diagnostics worker and lab supervisor sit outside consensus. See [transport and recovery](TRANSPORT_AND_RECOVERY.md), [lab checkpoints](LAB_CHECKPOINTS.md) and [verification status](VERIFICATION.md).
