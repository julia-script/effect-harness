/** Tasks are reduced before conversation edges; result and checkpoint payloads stay private. */
import * as Schema from 'effect/Schema'
import * as Record from '../Record.js'
import type * as Graph from '../TaskGraph.js'

const phase = Schema.is(Schema.Struct({ phase: Schema.String }))
export const project = (
  tasks: ReadonlyMap<Record.TaskId, Record.Task>,
  conversations: ReadonlyMap<Record.ConversationId, Record.Conversation>,
): Graph.Value => ({
  tasks: [...tasks.values()]
    .sort((a, b) => a.id - b.id)
    .flatMap((task): Graph.Node[] => {
      if (task.state.status === 'terminal') return []
      const checkpoint = task.state.checkpoint
      const outcome = task.state.outcome
      return [
        {
          id: task.id,
          conversationId: task.conversationId,
          kind: task.kind,
          version: task.version,
          ...(task.owner === undefined ? {} : { owner: task.owner }),
          background: task.background,
          abortRequested: task.abortRequested,
          status: task.state.status,
          ...(phase(checkpoint) ? { phase: checkpoint.phase } : {}),
          on: [...(task.state.on ?? [])],
          ...(task.state.policy === undefined ? {} : { policy: task.state.policy }),
          ...(Schema.is(Record.TaskOutcome)(outcome) ? { heldOutcomeStatus: outcome.status } : {}),
          ownedConversations: [...conversations.values()]
            .filter((c) => c.owner?.taskId === task.id)
            .map((c) => c.id)
            .sort((a, b) => a - b),
        },
      ]
    }),
  conversations: [...conversations.values()].sort((a, b) => a.id - b.id),
})
