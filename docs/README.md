# Documentation

The [docs app](../apps/docs/README.md) provides a Foldkit site with a guided tutorial, practical guides, reference pages, and a controlled-model conversation lab. Its [content plan](../apps/docs/CONTENT_PLAN.md) records audiences and Diátaxis scope.

Start with the [README examples](../README.md) or run the [offline application](../apps/example/README.md).

- [Tools](tools.md): schema declarations, handler Layers, dependencies, progress, and replay.
- [Providers](providers.md): native Effect AI models and explicit model descriptors.
- [Persistence](persistence.md): Storage, Session, documents, and atomic transactions.
- [Tracing](tracing.md): durable span names, correlation attributes, and Effect tracer setup.
- [Observations](observations.md): committed entries, document streams, and submission settlement.
- [Composition](explanation/composition.md): the client, backend, local runtime, and service requirements.
- [Recovery](explanation/recovery.md): restart, request identity, and interrupted actions.

The [example tour](../apps/example/TOUR.md) links complete programs for recovery, forks, documents, and extensions. All examples use the public package imports.
