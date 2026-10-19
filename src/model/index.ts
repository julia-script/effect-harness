/**
 * The model boundary: requests, responses, and the providers that answer them.
 */

/** The `Model` service, its requests and events, and the `Provider` interface backends implement. */
export * as Model from './Model.ts'

/** Model messages: the transcript a request carries and the responses entries store. */
export * as Message from './Message.ts'

/** The system prompt and tools in effect after a transcript's system messages. */
export * as Transcript from './Transcript.ts'

/** How a model request fails. */
export * as ModelError from './ModelError.ts'

/** A scripted provider for tests. */
export * as Faux from './Faux.ts'

/** A provider over an effect/ai `LanguageModel`. */
export * as LanguageModelProvider from './LanguageModelProvider.ts'
