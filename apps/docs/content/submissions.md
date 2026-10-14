# Submission reference

`effect-harness/Submission` represents a durable input or passive entry-write admission. A handle contains `id` and `conversationId`. The client functions below accept a handle directly or return a pipeable function when called without one.

This page covers draft fields and the `read`, `wait`, and `withdraw` operations. Conversation scheduling and runtime options are outside its scope.

## Input drafts

Passed to `Conversation.submit`. All fields are schema-validated.

| Field       | Type                                             | Behavior                                                                                                      |
| ----------- | ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------- |
| `type`      | `'input'`                                        | Required discriminant.                                                                                        |
| `content`   | `string` or native `Prompt.UserMessagePart[]`    | Required input content.                                                                                       |
| `requestId` | non-empty string, optional                       | Reuses the admission for that request identity within the conversation.                                       |
| `whenBusy`  | `'followUp'`, `'steer'`, or `'reject'`, optional | Defaults to follow-up scheduling. Steer joins at a turn boundary. Reject fails when the conversation is busy. |

A retry with the same identity retrieves the existing admission. Request identity is not an exactly-once guarantee for external tool effects.

## Write drafts

| Field       | Type                       | Behavior                                    |
| ----------- | -------------------------- | ------------------------------------------- |
| `type`      | `'write'`                  | Required discriminant.                      |
| `entry`     | `Transaction.EntryDraft`   | Required passive entry.                     |
| `requestId` | non-empty string, optional | Admission identity within the conversation. |

A passive write does not invoke the model. An entry draft includes its kind and JSON data; full entry contracts are defined in `Transaction`.

## read

`Submission.read(handle)` returns `Effect<Record, HarnessError>`. It reads the persisted current admission state. It does not wait for completion.

The record union is `InputQueued`, `InputPlaced`, `InputDone`, `InputUnanswered`, `WriteQueued`, `WriteDone`, or `WriteUnanswered`. The discriminant is `_tag`.

## wait

`Submission.wait(handle)` returns `Effect<Settled, HarnessError>`. It waits for a persisted terminal admission state. Local execution resumes pending work when necessary.

| Result            | Meaning                                                          |
| ----------------- | ---------------------------------------------------------------- |
| `InputDone`       | Completed input. `answer` identifies its committed answer entry. |
| `InputUnanswered` | Input settled without an answer.                                 |
| `WriteDone`       | Passive write placed.                                            |
| `WriteUnanswered` | Passive write settled without placement.                         |

Interrupting the wait cancels observation; it does not cancel the durable execution. Backend read/wait failures use `HarnessError`.

## withdraw

`Submission.withdraw(handle)` returns `Effect<Withdrawal, HarnessError>`.

| Result             | Meaning                                                    |
| ------------------ | ---------------------------------------------------------- |
| `'aborted'`        | The queued admission was withdrawn.                        |
| `'already_placed'` | The admission has entered execution and was not withdrawn. |
| `'settled'`        | The admission is already terminal.                         |

Withdrawal does not abort already placed work. `Conversation.abort` is the conversation execution operation.

## Related material

[Read a stored answer](/guides/observation) provides a complete consumer. [Restart and recovery](/explanation/recovery) explains request identity across runtime lifetimes.
