<!--
aks3: S3-compatible object storage server
Copyright (C) 2026 aks3 contributors
SPDX-License-Identifier: AGPL-3.0-only
-->

# proptest regressions

When a property test fails, proptest writes the seed of the failing case into
`<module>.txt` in this directory and replays it before any new case on every
later run. `paths.txt` holds the seeds for `src/paths.rs`, `fs_engine.txt` for
`src/fs_engine.rs`, and so on.

**These files are committed, not ignored.** A property finds a case once, by
chance, out of an input space nobody can enumerate; the seed is what turns that
one lucky case into a permanent test. Dropping the file throws the finding away.
Review a regression file in a diff the way you would review a test: the comment
proptest appends to each line names the shrunk input, so it should be readable
as a claim about what used to be broken.

CI runs with `PROPTEST_CASES` pinned in `.github/workflows/ci.yml`, above the
local default, so a case can fail there and not here. Every failure prints a
`cc <seed>` line for exactly that situation: paste it into the file named in the
same output and the failure reproduces locally, at any case count.

This README only keeps the directory in git before the first seed lands.
proptest ignores it.
