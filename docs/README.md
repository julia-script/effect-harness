# Effect Harness documentation

Build AI applications with ordinary Effect services, native AI models and tools, and native Workflows. These docs assume you know TypeScript and Effect's Effect, Service, Layer and Scope APIs.

The generic harness prepares prompts and executes models and tools. The durable package adds committed conversation state and executor Layers. Your application supplies the provider, persistence services and WorkflowEngine.

## Install and run examples

Use your preferred package manager. Shell recipes below use npm syntax; the equivalent `pnpm add`, `yarn add` and `bun add` commands install the same packages. Choose only the adapters you need.

The tutorials run TypeScript with Node.js and `tsx`. Platform services are explicit Layers; use your runtime's adapter when integrating the library into an existing application. Installation commands leave dependency versions to your package manager. The [compatibility reference](reference/compatibility.md) records the specific dependency behavior required by the library.

## Start with a working application

[Run your first durable conversation](tutorials/first-conversation.md) builds a local model, binds a tool, submits a message and replays its receipt. It needs no API key. Then [save application state across restarts](tutorials/persistent-state.md) introduces schema-backed documents and persistent transactions.

## Integrate the pieces you need

| Goal                                                   | Guide                                                |
| ------------------------------------------------------ | ---------------------------------------------------- |
| Run your own Workflow alongside conversation execution | [Compose native Workflows](workflows.md)             |
| Use OpenAI or Anthropic inference                      | [Connect a model provider](providers.md)             |
| Let users authorize their own accounts                 | [Add account sign-in](how-to/account-sign-in.md)     |
| Expose application functions or coding tools           | [Register tools](tools.md)                           |
| Recover conversations after process restarts           | [Persist domain state and execution](persistence.md) |
| Display committed conversation progress                | [Observe a conversation](observations.md)            |

## Look up a contract

[Packages and services](reference/packages.md) maps the public modules and their Layer dependencies. [Configuration](reference/configuration.md) records defaults and policy precedence. [Documents and storage](reference/documents-and-storage.md) describes addresses, migrations, transactions and failure certainty. [Execution and observations](reference/execution-and-observation.md) covers submission identity, receipts, watches and lifetime rules. [Effect compatibility](reference/compatibility.md) records dependency requirements.

Individual exports are documented in their TypeScript API comments. The reference pages describe the contracts that span those exports.

## Understand the design

[About composition](explanation/composition.md) connects model execution, conversation state and native Workflow registration. [About replay and recovery](explanation/recovery.md) follows a request across commit boundaries and explains what happens when a process stops between them.
