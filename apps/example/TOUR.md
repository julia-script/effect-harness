# Conversation tour

Build and run the offline tour:

```sh
bun run build
bun run --cwd apps/example tour
```

The tour runs three independent programs using the public API:

| Example                              | Behavior                                                                                          |
| ------------------------------------ | ------------------------------------------------------------------------------------------------- |
| [Forks](src/tour/Forks.ts)           | Create a new conversation at an answer, configure it, and submit to parent and fork concurrently. |
| [Documents](src/tour/Documents.ts)   | Update schema-backed application state and append an entry atomically from a tool handler.        |
| [Extensions](src/tour/Extensions.ts) | Bundle tools, a prompt section, and a before-tool hook with deferred handler provisioning.        |

[Recovery](src/tour/Recovery.ts) is a separate single-file entrypoint using persistent SQLite storage:

```sh
bun run --cwd apps/example recovery
```

Run that same command after an interruption. The runtime resumes persisted work, and the same request ID retrieves the same submission. The executable model is local and deterministic so every example runs without credentials.
