# How the harness is composed

The harness is an embedded runtime. Its host owns the process, Layer graph, store and execution environment. Conversation handles route operations to that live owner.

Persistence stores domain facts and task checkpoints together. The scheduler reserves work under a serialized transaction, runs the phase in an Effect fiber, then commits its next checkpoint or outcome. Live fibers and notifications are replaceable coordination structures; stored task records establish recoverable work.

Executor prepares prompts and uses native Effect AI models and Toolkits. Registry supplies executable tools, sections and hooks. Persisted records refer to stable names and configuration, while the host reinstalls executable definitions after reopening.

An application can expose submit/read/abort/watch over its own server. Multiple remote clients can use one harness owner without opening its database or becoming execution workers.
