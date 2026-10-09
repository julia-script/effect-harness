# Client and runtime composition

`Harness` is the application-facing service. Its root/create/conversation operations return lightweight conversation handles; those handles hold identity and a backend connection. They do not expose Storage, Session, tools, or models.

`HarnessBackend` defines the data operations required by the client: conversation creation and lookup, input submission, settlement, configuration, forks, aborts, entries, and document observation. Requests and responses carry schema-backed data. Arbitrary transaction callbacks do not cross this boundary.

`HarnessRuntime` implements that backend locally. It owns one Session, scoped workers, model requests, tool execution, durable checkpoints, and shutdown. `Harness.layerLocal` supplies both the backend and client in one process. `Harness.layer` can instead receive a separately supplied backend.

`Session` coordinates persisted state. It can be used without HarnessRuntime for reading and updating application documents. A transaction stages facts; Storage persists the batch; Session adopts the committed state and publishes observations.

Tool declarations are independent of handler Layers. Services captured by handler builders determine where operations happen. There is no separate environment abstraction; the application's Layers resolve filesystem, network, provider, and application-service requirements.

The current implementation is local. A future transport can implement HarnessBackend without exposing runtime dependencies to the client. That separation does not itself provide distributed execution, shared storage ownership, or automatic scheduling between machines.

A local runtime processes one submission at a time in each conversation. Independent conversations can run concurrently. Constructing the runtime stays idle. Accessing the root, creating a conversation, or successfully looking up a saved conversation activates the runtime and schedules persisted pending/running work. Submitting input and waiting for idle also activate scheduling. For storage-only inspection without executing agents, use Session independently.
