# Maintaining QuorumKV

This repository is maintained by [@pavann19](https://github.com/pavann19). CODEOWNERS routes ownership; it does not establish an independent review. Sole-maintainer self-review must not be presented as approval by a second engineer.

## Main and pull requests

`main` is the canonical integration branch. Implement on a short-lived branch, review the full diff and wait for required CI against the current head and base. Resolve conversations before merging. Keep required checks, admin enforcement and force-push/deletion protection in place. Do not use an admin bypass to turn a failed check into a merge.

Use a merge commit when preserving useful, buildable commits from an integration PR. Avoid duplicate merges of superseded candidates. Delete a merged remote feature branch after verifying `main`; retain unrelated unmerged work. Never delete runtime data or failed evidence as branch housekeeping.

## Evidence and dependencies

Keep [verification status](docs/VERIFICATION.md) as a dated source snapshot. For a new claim, link the exact commit, CI run or reproducible evidence and state its limits. Failed and incomplete acceptance goals remain visible after a source merge. Do not publish a production-ready claim or release tag merely because CI is green.

Review dependencies and Actions updates in a focused PR. Keep actions pinned, preserve lock/version files and inspect vulnerability and license effects. Do not suppress a security gate through a broad allowlist. Secrets go through [private reporting](SECURITY.md), never an issue attachment.

## Release checklist

- Confirm the intended source commit and passing checks, including optional workers affected by the change.
- Review API/configuration compatibility, data migrations and recovery instructions on disposable stores.
- Check source and reachable history for secrets and unintended artifacts; an ignore rule does not scrub history.
- Recheck [LICENSE](LICENSE), [NOTICE](NOTICE) and [dependency notices](THIRD_PARTY_NOTICES.txt). Binary/container distributions can require additional license/source delivery.
- Describe verified changes and unresolved failures. Attach artifact identities and applicable scans if publishing artifacts.
- Treat a release, cloud deployment or endurance experiment as a separate deliberate operation. Ordinary CI does not authorize paid infrastructure.

## Incident handling

Preserve the failing source/configuration and sanitized symptoms before repair. For an uncertain write, investigate its durable identity/state instead of assuming rollback. Follow the project's recovery guide; never let an AI worker perform domain repairs autonomously. Verify the repair separately and retain the original failure as historical evidence.
