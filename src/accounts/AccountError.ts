/**
 * How an account operation fails. The `reason` says what to show the person, and `message` says it
 * in words.
 */
import { Schema } from 'effect'

/** Another account holds the name. */
export class NameTaken extends Schema.TaggedError<NameTaken, { readonly brand: unique symbol }>(
  'effect-harness/accounts/AccountError/NameTaken',
)('NameTaken', {}) {}

/** The adapter is not one the host lists. */
export class UnknownAdapter extends Schema.TaggedError<
  UnknownAdapter,
  { readonly brand: unique symbol }
>('effect-harness/accounts/AccountError/UnknownAdapter')('UnknownAdapter', {
  adapter: Schema.String,
}) {}

/** The settings do not decode with the adapter's Schema. */
export class InvalidSettings extends Schema.TaggedError<
  InvalidSettings,
  { readonly brand: unique symbol }
>('effect-harness/accounts/AccountError/InvalidSettings')('InvalidSettings', {}) {}

/** The adapter does not take this operation, such as a key for a device-code adapter. */
export class Unsupported extends Schema.TaggedError<Unsupported, { readonly brand: unique symbol }>(
  'effect-harness/accounts/AccountError/Unsupported',
)('Unsupported', {}) {}

/** The provider refused the credential while listing models with it. */
export class VerifyFailed extends Schema.TaggedError<
  VerifyFailed,
  { readonly brand: unique symbol }
>('effect-harness/accounts/AccountError/VerifyFailed')('VerifyFailed', {}) {}

/** The sign-in expired, was denied, or its upstream failed. */
export class SignInFailed extends Schema.TaggedError<
  SignInFailed,
  { readonly brand: unique symbol }
>('effect-harness/accounts/AccountError/SignInFailed')('SignInFailed', {}) {}

/** Another stream already owns a sign-in for this account name. */
export class SignInActive extends Schema.TaggedError<
  SignInActive,
  { readonly brand: unique symbol }
>('effect-harness/accounts/AccountError/SignInActive')('SignInActive', {}) {}

/** The owning stream or host cancelled the attempt. */
export class SignInCancelled extends Schema.TaggedError<
  SignInCancelled,
  { readonly brand: unique symbol }
>('effect-harness/accounts/AccountError/SignInCancelled')('SignInCancelled', {}) {}

/** The attempt is unknown, finished, or does not accept this operation. */
export class InvalidSignIn extends Schema.TaggedError<
  InvalidSignIn,
  { readonly brand: unique symbol }
>('effect-harness/accounts/AccountError/InvalidSignIn')('InvalidSignIn', {}) {}

/** The target account or its credential changed while sign-in was in progress. */
export class SignInChanged extends Schema.TaggedError<
  SignInChanged,
  { readonly brand: unique symbol }
>('effect-harness/accounts/AccountError/SignInChanged')('SignInChanged', {}) {}

/** No account has the name. */
export class NotFound extends Schema.TaggedError<NotFound, { readonly brand: unique symbol }>(
  'effect-harness/accounts/AccountError/NotFound',
)('NotFound', {}) {}

/** The account store or the credential store failed. */
export class StoreFailed extends Schema.TaggedError<StoreFailed, { readonly brand: unique symbol }>(
  'effect-harness/accounts/AccountError/StoreFailed',
)('StoreFailed', {}) {}

export const Reason = Schema.Union([
  NameTaken,
  UnknownAdapter,
  InvalidSettings,
  Unsupported,
  VerifyFailed,
  SignInFailed,
  SignInActive,
  SignInCancelled,
  InvalidSignIn,
  SignInChanged,
  NotFound,
  StoreFailed,
])
export type Reason = typeof Reason.Type

export class AccountError extends Schema.TaggedError<
  AccountError,
  { readonly brand: unique symbol }
>('effect-harness/accounts/AccountError/AccountError')('AccountError', {
  message: Schema.String,
  reason: Reason,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}
