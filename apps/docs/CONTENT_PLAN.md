# Documentation plan

Audience confirmed by Julia: TypeScript developers who know basic Effect.
Destination: the Foldkit docs application in `apps/docs`, with Markdown content and source-backed TypeScript examples.

## Audiences

- Evaluators know TypeScript and basic Effect. They need to see whether durable conversations fit their application. The overview, lab, first-conversation tutorial, and recovery explanation serve them.
- Integrators know Effect, Layer, Schema, and their own model provider. They need to add tools, display results, and persist application state. Goal-oriented guides and complete examples serve them.
- Experienced users need exact constraints while working. Submission and storage references serve them; the implementation links provide a path beyond this deliberately scoped reference set.

## Documents

| Page                                     | Quadrant                | Audience                | Scope and assumed knowledge                                                                                              |
| ---------------------------------------- | ----------------------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| Your first conversation                  | Tutorial                | Evaluators, integrators | Create one offline program, read an answer, retry an admission. Assumes basic Effect and Bun.                            |
| Add a tool                               | How-to                  | Integrators             | Wire a schema-backed handler to a model. Assumes a native model Layer.                                                   |
| Read a stored answer and follow progress | How-to                  | Integrators             | Decode one answer and observe committed work. Assumes existing conversation/submission handles.                          |
| Preserve conversations across restarts   | How-to                  | Integrators             | Compose SQLite storage and reopen a request. Assumes Node and Layer composition.                                         |
| Connect a model                          | Existing provider guide | Integrators             | Native providers and descriptors; retain current source content initially. Assumes provider credentials and HTTP Layers. |
| Submission reference                     | Reference               | Experienced users       | Draft fields and read/wait/withdraw; all options for that scope. Assumes conversation client use.                        |
| Storage layer reference                  | Reference               | Experienced users       | Built-in constructors, requirements, failures, and lifetime. Assumes Layers and storage ownership.                       |
| Client and runtime                       | Explanation             | Evaluators, integrators | Service boundaries and local execution; reuse existing source.                                                           |
| Restart and recovery                     | Explanation             | All three               | Checkpoints, replay, uncertain effects, and scope shutdown; reuse existing source.                                       |

The overview is an orientation page. The lab is an interactive learning companion, not API reference. Complete example source pages are supplementary examples linked from the appropriate quadrant.

## Interactive examples

The lab begins as a familiar coding chat: send a question about a failing login test, see the assistant read the fixture, and watch its reply stream automatically. A suggested follow-up asks for the fix. The model is explicitly scripted, not a general-purpose assistant.

“Under the hood” is an opt-in explanation of the same events: save the user message, run and record the tool, then save the answer. Readers can pause and step the stream or stop and resume the runtime. A second disclosure contains raw entries, committed partials, request-ID reuse, and conversation forks. This progression introduces each concept through something the reader already saw in the chat.

The demo runs the real `effect-harness` package with an offline native `LanguageModel`. Deferred gates control tool intent, tool completion, and text chunks. Chat history is projected from stored model messages; the current reply uses the provider stream until an answer is committed. Memory storage is page-local; the SQLite example demonstrates disk recovery.

## Verification

TypeScript snippets are embedded from files included in typechecking or the existing example application. Execute the standalone tutorial in a clean directory against its pinned published versions. Integration tests exercise demo gating, recovery, idempotent admission, and fork isolation. Exercise the UI on desktop and mobile, then verify Cloudflare deep links after deploying through Alchemy.

## Deliberate omissions

No Effect fundamentals course, contributor handbook, or exhaustive API encyclopedia in this first set. No paid model calls in public demos. No claims that the browser memory store survives refresh, that request IDs make external effects exactly once, or that local runtime storage supports multiple active owners.
