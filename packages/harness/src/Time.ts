import * as DateTime from 'effect/DateTime'
import * as Duration from 'effect/Duration'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import * as SchemaIssue from 'effect/SchemaIssue'

const origin = DateTime.fromEpochSeconds(0)
/** Native finite epoch milliseconds retain submillisecond precision. */
export const fromEpochMillis = (millis: number): DateTime.Utc =>
  DateTime.mapEpochMillis(origin, () => millis)
/** Domain UTC instant with its original finite numeric epoch-millisecond encoding. */
export const EpochMillis = Schema.Finite.pipe(
  Schema.decodeTo(Schema.DateTimeUtc, {
    decode: SchemaGetter.transform(fromEpochMillis),
    encode: SchemaGetter.transform(DateTime.toEpochMillis),
  }),
)
const finiteSpan = Schema.Duration.check(
  Schema.makeFilter((value) => Duration.isFinite(value) || 'Expected a finite duration'),
)
export const DurationMillis = Schema.Finite.pipe(
  Schema.decodeTo(finiteSpan, {
    decode: SchemaGetter.transform(Duration.millis),
    encode: SchemaGetter.transform(Duration.toMillis),
  }),
)
export const DurationSeconds = Schema.Finite.pipe(
  Schema.decodeTo(finiteSpan, {
    decode: SchemaGetter.transform(Duration.seconds),
    encode: SchemaGetter.transform(Duration.toSeconds),
  }),
)
const nonnegativeSpan = finiteSpan.check(
  Schema.makeFilter((value) => {
    const millis = Duration.toMillis(value)
    return (
      (Number.isSafeInteger(millis) && millis >= 0) ||
      'Expected nonnegative safe integer milliseconds'
    )
  }),
)
export const NonnegativeMillis = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)).pipe(
  Schema.decodeTo(nonnegativeSpan, {
    decode: SchemaGetter.transform(Duration.millis),
    encode: SchemaGetter.transform(Duration.toMillis),
  }),
)
const commandSpan = finiteSpan.check(
  Schema.makeFilter((value) => {
    const millis = Duration.toMillis(value)
    return (
      (millis > 0 && millis <= 2147483647) ||
      'Expected a positive command timeout within 2147483647 milliseconds'
    )
  }),
)
/** Model tool JSON keeps positive finite seconds while the domain uses Duration. */
export const CommandTimeout = Schema.Finite.check(
  Schema.isGreaterThan(0),
  Schema.isLessThanOrEqualTo(2147483.647),
).pipe(
  Schema.decodeTo(commandSpan, {
    decode: SchemaGetter.transform(Duration.seconds),
    encode: SchemaGetter.transform(Duration.toSeconds),
  }),
)
/** Runtime option boundary; native fromInput failures and nonfinite spans become SchemaError. */
export const DurationInput = Schema.Unknown.pipe(
  Schema.decodeTo(finiteSpan, {
    decode: SchemaGetter.transformEffect((input, options) =>
      Effect.gen(function* () {
        if (typeof input === 'number' && !Number.isFinite(input))
          return yield* Effect.fail(
            new SchemaIssue.InvalidValue(
              { message: 'Expected a finite duration input' },
              input,
              options,
            ),
          )
        const decoded = yield* Effect.try({
          // fromInput returns None for parse exceptions; canonicalization preserves exact nanos and also guards foreign Duration lookalikes/getters.
          try: () =>
            Option.flatMap(
              Duration.fromInput(input as Duration.Input),
              (value) =>
                Duration.match(value, {
                  onMillis: (millis) =>
                    Number.isFinite(millis) ? Option.some(Duration.millis(millis)) : Option.none(),
                  onNanos: (nanos) =>
                    typeof nanos === 'bigint' ? Option.some(Duration.nanos(nanos)) : Option.none(),
                  onInfinity: () => Option.none(),
                  onNegativeInfinity: () => Option.none(),
                }) ?? Option.none(),
            ),
          catch: (cause) =>
            new SchemaIssue.InvalidValue(
              { message: 'Invalid duration input', cause },
              input,
              options,
            ),
        })
        return Option.isSome(decoded)
          ? decoded.value
          : yield* Effect.fail(
              new SchemaIssue.InvalidValue({ message: 'Invalid duration input' }, input, options),
            )
      }),
    ),
    encode: SchemaGetter.passthroughSubtype<unknown, Duration.Duration>(),
  }),
)
export const duration = Schema.decodeUnknownEffect(DurationInput)
