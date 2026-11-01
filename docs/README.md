# QuorumKV documentation

Start with the [project overview](../README.md) and [verification status](VERIFICATION.md). The latter identifies the exact reviewed source revision and separates hosted checks from operational evidence.

## Guides

- [ARCHITECTURE](ARCHITECTURE.md)
- [RAFT BOUNDARY](RAFT_BOUNDARY.md)
- [TRANSPORT AND RECOVERY](TRANSPORT_AND_RECOVERY.md)
- [TESTING](TESTING.md)
- [DEMO](DEMO.md)
- [CORRECTNESS CHECKS](CORRECTNESS_CHECKS.md)
- [LAB CHECKPOINTS](LAB_CHECKPOINTS.md)
- [README](../diagnostics/README.md)

## Reading paths

For evaluation, read architecture, verification status and test limitations. For installation, follow the deployment or demo guide before changing an environment. For maintenance, read recovery instructions and the optional worker guide separately: the worker is not the authority for admission, consensus or financial state.

Commands run from the repository root unless stated otherwise. Historical measurements describe their original revision and environment; they are not current capacity or production availability claims. A finite passing test is not a 24-hour reliability result. Preserve failed evidence when verifying a repair.

Source licensing is in [LICENSE](../LICENSE) and [third-party notices](../THIRD_PARTY_NOTICES.txt). Review distribution obligations again when producing binaries or containers.

Contributor workflow: [contributing](../CONTRIBUTING.md), [maintenance](../MAINTAINING.md), [security reporting](../SECURITY.md).
