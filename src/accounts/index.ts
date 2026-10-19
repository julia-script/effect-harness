/**
 * Accounts: the provider types a host supports, declared once, and their instances, added while
 * it runs.
 */

/** Adapter declarations, groups of them, and their implementations. */
export * as Adapter from './Adapter.ts'

/** Accounts as data: what is stored, reported, and sent to add one. */
export * as Account from './Account.ts'

/** The account registry service and its layers. */
export * as Accounts from './Accounts.ts'

/** The protocol a client manages accounts over. */
export * as AccountsRpcs from './AccountsRpcs.ts'

/** How an account operation fails. */
export * as AccountError from './AccountError.ts'
