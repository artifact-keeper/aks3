# aks3 (working name)

A single-binary S3-compatible object store written in Rust, derived from MinIO's
engine design. What sets it apart from RustFS (Apache-2.0, young) and Garage
(AGPL, resilience-focused, eventually consistent) is behavioral fidelity to
MinIO, strong consistency, and a published, CI-enforced compliance matrix.

## Origin and license

aks3 is licensed under the GNU Affero General Public License v3.0 only. The full
text is in `LICENSE`.

Parts of the storage engine, IAM policy evaluation, and encryption design are
derived from MinIO. See `NOTICE` for the attribution, the pinned reference
commit, and the trademark statement.

## Status

Status: pre-alpha, Phase 0.
