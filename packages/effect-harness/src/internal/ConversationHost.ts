/** Client handle construction captures only a backend contract, never a Session or runtime. */
import { pipeArguments } from 'effect/Pipeable'
import type * as Record from '../Record.js'
import type * as Conversation from '../Conversation.js'
import type * as Submission from '../Submission.js'
import type { HarnessBackendService } from '../HarnessBackend.js'
import { Backend, ConversationTypeId, SubmissionTypeId } from './ClientHandle.js'

export { Backend } from './ClientHandle.js'
export const conversation = (
  backend: HarnessBackendService,
  id: Record.ConversationId,
): Conversation.Conversation => ({
  [ConversationTypeId]: ConversationTypeId,
  [Backend]: backend,
  id,
  pipe() {
    return pipeArguments(this, arguments)
  },
})
export const submission = (
  backend: HarnessBackendService,
  id: Record.SubmissionId,
  conversationId: Record.ConversationId,
): Submission.Submission => ({
  [SubmissionTypeId]: SubmissionTypeId,
  [Backend]: backend,
  id,
  conversationId,
  pipe() {
    return pipeArguments(this, arguments)
  },
})
