# Effect compatibility

The package uses Effect 4.0.1 and native Effect AI model and Toolkit APIs. Runtime-specific platform and storage adapters are composed by the application.

## Model-response validation

Native Effect AI validates model responses against offered tools and parameter schemas. An unknown tool call can be rejected before the harness receives a usable call. The harness records the failed attempt rather than bypassing native validation.

The offline unknown-tool example and native validation tests make that boundary explicit. Supported native APIs determine the available response parts and metadata. Providers still decide model availability and supported media.

## Stored formats

Task definitions have explicit names and versions. Persistence has a fresh experimental format. The harness does not guess an older execution model or import previous stores automatically.

## Environment recovery

Persistent task state does not reconstruct a working directory, installed tools or remote sandbox. The application supplies those capabilities when reopening. Work requiring external side effects must declare its replay policy.
