/** A persisted submission identity and its live owning capability. */
import type * as Record from './Record.ts'
import type * as Harness from './Harness.ts'
import * as Schema from 'effect/Schema'
export {
  Submission as Record,
  SubmissionId as Id,
  InputQueued,
  InputPlaced,
  InputDone,
  InputUnanswered,
  WriteQueued,
  WriteDone,
  WriteUnanswered,
} from './Record.ts'
export interface Submission {
  readonly id: Record.SubmissionId
  readonly harness: Harness.Service
}
export const wait = (self: Submission) => self.harness.awaitSubmission(self.id)
export const read = (self: Submission) => self.harness.submission(self.id)

/** Queued withdrawal settles durably; placed input remains owned by its turn. */
export const WithdrawalResult = Schema.Literals(['aborted', 'already_placed', 'settled'])
export type WithdrawalResult = typeof WithdrawalResult.Type
export const withdraw = (self: Submission) => self.harness.withdraw(self.id)
