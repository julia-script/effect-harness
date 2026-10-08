/** Private native model factory, captured by an owning provider construction. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as LanguageModel from 'effect/ai/LanguageModel'

export class NativeLanguageModel extends Context.Service<
  NativeLanguageModel,
  {
    readonly make: typeof LanguageModel.make
  }
>()('effect-harness/internal/NativeLanguageModel') {}
export const factory = Effect.map(Effect.serviceOption(NativeLanguageModel), (service) =>
  Option.getOrElse(service, () => NativeLanguageModel.of({ make: LanguageModel.make })),
)
