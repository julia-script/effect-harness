# Read a stored answer and follow progress

Use this guide to display the answer to an existing submission and to observe entries as they commit. It assumes you already hold a conversation and a submission from that conversation.

## Read the final answer

Add this module to your application. `readAnswer` waits for settlement, locates the answer entry, and decodes its native Effect AI messages. It returns the settlement status and the concatenated text parts.

<!-- example:observe -->

Pass the conversation and its submission to `readAnswer`. Check `status` before treating `text` as a successful answer. `InputUnanswered` is a settled outcome, not an answer.

## Follow committed entries

Run `logCommittedEntries` in a fiber owned by your application's scope. The stream first emits visible history, then follows new entries. It does not end merely because one answer finishes; interrupt the observer when your view closes.

To reconnect after a known entry, pass `{ after: entryId }` as the second argument to `Conversation.entries`. Store a real entry ID from this conversation rather than inventing a sequence number.

## Choose a UI projection

Use `Conversation.view` for a coherent current snapshot, or `Conversation.watchView` for ongoing frames that include entries, live model progress, and task state. A `Delta` carries replacement operations; reduce frames with `ConversationView.reduce` when maintaining a local projection.

The [conversation lab](/lab) periodically reads `Conversation.view`. It shows provider chunks separately from the last committed partial, so the UI makes the persistence boundary visible.

Interrupting `Submission.wait` ends observation. To stop execution, use `Conversation.abort`. To remove input that is still queued, use `Submission.withdraw`. See the [submission reference](/reference/submissions) for the results and constraints.
