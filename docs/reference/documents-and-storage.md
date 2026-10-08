# Documents and storage

Persistence stores conversations, entries, tasks, submissions, document membership and document values/history. Record reads return detached values; scans use Streams with bounded backend fetches.

## Transactions

A transaction stages a batch. Admission, identifier allocation, document edits, transcript results and task outcomes can share that batch. A successful commit supplies one ordered revision. Scheduler and observer publication follow confirmed persistence.

Document drafts exist only inside their transaction callback. They are revoked afterward. Keep model inference, shell commands and network requests outside transaction callbacks; the built-in model and tool handlers run outside the mutation line.

A rejected write has no durable effect. An uncertain write may have committed; the open runtime stops accepting writes so reopening can reconstruct authoritative state.

## Document definitions

Documents declare a stable kind, version, ownership scope, JSON-object codec and initial value. Conversation documents can retain `latest` or `rewindable` history. Fork policy is `asOf`, `current` or `initial`.

Retiring a document removes its active incarnation; recreation gets a new identity. Task-scoped progress documents are retired at terminal settlement. Historical membership is separate from current address lookup.

## Scans and adapters

An entry scan captures an upper ID cutoff. Later appends are outside that scan. Task discovery scans read current committed records, and the scheduler rechecks eligibility on the mutation line before reservation.

SQLite adapters use indexed record access. JSONL adapters write one framed atomic commit per line, replay complete frames on opening and discard an incomplete tail. They hold rebuilt indexes in memory. By default JSONL syncs the data file and directory before acknowledging a commit; `fsync: false` opts out. With synchronization enabled, the filesystem must support file and directory synchronization; failures reject acquisition or leave a commit uncertain and stop further writes in the open runtime. A separate `owner.sqlite` file supplies the process ownership lock; conversation records remain in JSONL. The journal is append-only and currently has no reclamation or checkpoint compaction, so opening time and memory use grow with retained history. Applications still determine how their storage survives machine loss.

Memory implements the same commit interface without surviving process exit. One store has one owning harness. Reopening requires compatible stored schemas and installed task definitions.
