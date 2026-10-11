/** Pure translation of two trusted complete states, never callbacks or provider intent. */
import * as Schema from 'effect/Schema'
import type * as View from '../ConversationView.js'
import * as ViewSchema from '../ConversationView.js'
import type * as Events from '../AgentEvent.js'
import * as EventSchema from '../AgentEvent.js'

const same = (a: View.Value, b: View.Value) => Schema.toEquivalence(ViewSchema.ValueSchema)(a, b)
const sameEvent = (a: Events.Event, b: Events.Event) =>
  Schema.toEquivalence(EventSchema.EventSchema)(a, b)
export const equal = same
export const translate = (before: View.Value, after: View.Value): ReadonlyArray<Events.Event> => {
  const events: Events.Event[] = []
  const changed = (event: Events.Event, previous?: Events.Event) => {
    if (previous === undefined || !sameEvent(event, previous)) events.push(event)
  }
  const outcome = (id: number) => after.terminal.find((t) => t.taskId === id)
  for (const p of after.progress)
    changed(
      { _tag: 'Progress', ...p },
      before.progress.find((old) => old.key === p.key) === undefined
        ? undefined
        : { _tag: 'Progress', ...before.progress.find((old) => old.key === p.key)! },
    )
  const depth = (id: number): number => {
    const seen = new Set<number>()
    let at: number | undefined = id
    while (at !== undefined && !seen.has(at)) {
      seen.add(at)
      at = after.nested.find((n) => n.taskId === at)?.parentTaskId
    }
    return seen.size
  }
  for (const n of [...after.nested].sort(
    (a, b) => depth(b.taskId) - depth(a.taskId) || a.taskId - b.taskId,
  )) {
    const old = before.nested.find((item) => item.taskId === n.taskId)
    if (
      n.availability !== 'pending' &&
      (old === undefined || !Schema.toEquivalence(ViewSchema.NestedSchema)(n, old))
    )
      events.push({ _tag: 'NestedEnd', nested: n })
  }
  const ending: Events.Event[] = []
  for (const round of after.rounds)
    for (const slot of round.slots) {
      const old = before.rounds
        .find((r) => r.key === round.key)
        ?.slots.find((s) => s.callId === slot.callId)
      if (
        (slot.result !== undefined && old?.result === undefined) ||
        (slot.result === undefined &&
          slot.taskId !== undefined &&
          old?.taskId !== undefined &&
          outcome(slot.taskId) !== undefined &&
          !before.terminal.some((t) => t.taskId === slot.taskId))
      )
        ending.push({
          _tag: 'ToolEnd',
          ownerTaskId: round.ownerTaskId,
          slot,
          ...(slot.taskId === undefined || outcome(slot.taskId) === undefined
            ? {}
            : { outcome: outcome(slot.taskId)! }),
        })
    }
  const emitted = new Set<Events.Event>()
  for (const entry of after.entries.filter((e) => !before.entries.some((old) => old.id === e.id))) {
    for (const event of ending)
      if (event._tag === 'ToolEnd' && event.slot.entryId === entry.id) {
        events.push(event)
        emitted.add(event)
      }
    events.push({ _tag: 'Message', entry })
  }
  for (const event of ending) if (!emitted.has(event)) events.push(event)
  for (const item of before.compactions.items)
    if (!after.compactions.items.some((i) => i.taskId === item.taskId))
      events.push({
        _tag: 'CompactionEnd',
        taskId: item.taskId,
        ...(outcome(item.taskId) === undefined ? {} : { outcome: outcome(item.taskId)! }),
      })
  for (const t of after.terminal)
    if (!before.terminal.some((old) => old.taskId === t.taskId))
      events.push({ _tag: 'TaskEnd', outcome: t })
  if (
    before.live.generation !== undefined &&
    before.live.generation.requestId !== after.live.generation?.requestId
  )
    events.push({ _tag: 'GenerationEnd', generation: before.live.generation })
  for (const g of before.generations)
    if (
      g.ordinal !== undefined &&
      before.run.active?.taskId === g.taskId &&
      (after.run.active?.taskId !== g.taskId ||
        !after.generations.some((next) => next.taskId === g.taskId && next.ordinal === g.ordinal))
    )
      events.push({ _tag: 'TurnEnd', generation: g })
  if (before.run.active !== undefined && before.run.active.taskId !== after.run.active?.taskId)
    events.push({
      _tag: 'RunEnd',
      membership: before.run.active,
      ...(outcome(before.run.active.taskId) === undefined
        ? {}
        : { outcome: outcome(before.run.active.taskId)! }),
    })
  for (const s of after.submissions)
    changed(
      { _tag: 'Submission', submission: s },
      before.submissions.find((old) => old.id === s.id) === undefined
        ? undefined
        : { _tag: 'Submission', submission: before.submissions.find((old) => old.id === s.id)! },
    )
  changed({ _tag: 'Inbox', value: after.inbox }, { _tag: 'Inbox', value: before.inbox })
  changed({ _tag: 'Agent', value: after.agent }, { _tag: 'Agent', value: before.agent })
  changed({ _tag: 'Usage', value: after.usage }, { _tag: 'Usage', value: before.usage })
  for (const c of after.graph.conversations)
    changed(
      { _tag: 'Conversation', conversation: c },
      before.graph.conversations.find((old) => old.id === c.id) === undefined
        ? undefined
        : {
            _tag: 'Conversation',
            conversation: before.graph.conversations.find((old) => old.id === c.id)!,
          },
    )
  for (const s of after.subagents)
    changed(
      { _tag: 'Subagent', value: s },
      before.subagents.find((old) => old.handle.taskId === s.handle.taskId) === undefined
        ? undefined
        : {
            _tag: 'Subagent',
            value: before.subagents.find((old) => old.handle.taskId === s.handle.taskId)!,
          },
    )
  for (const item of after.compactions.items) {
    const old = before.compactions.items.find((i) => i.taskId === item.taskId)
    if (old === undefined) events.push({ _tag: 'CompactionStart', item })
    else changed({ _tag: 'CompactionUpdate', item }, { _tag: 'CompactionUpdate', item: old })
  }
  if (after.run.active !== undefined && before.run.active?.taskId !== after.run.active.taskId)
    events.push({ _tag: 'RunStart', membership: after.run.active })
  for (const g of after.generations) {
    const old = before.generations.find((p) => p.taskId === g.taskId)
    if (
      g.ordinal !== undefined &&
      after.run.active?.taskId === g.taskId &&
      (before.run.active?.taskId !== g.taskId || old === undefined || old.ordinal !== g.ordinal)
    )
      events.push({ _tag: 'TurnStart', generation: g })
    changed(
      { _tag: 'Phase', generation: g },
      old === undefined ? undefined : { _tag: 'Phase', generation: old },
    )
  }
  if (after.live.generation !== undefined) {
    if (before.live.generation?.requestId !== after.live.generation.requestId)
      events.push({ _tag: 'GenerationStart', generation: after.live.generation })
    else
      changed(
        { _tag: 'Partial', generation: after.live.generation },
        { _tag: 'Partial', generation: before.live.generation },
      )
  }
  for (const round of after.rounds)
    for (const slot of round.slots)
      if (
        slot.taskId !== undefined &&
        !before.rounds.some(
          (r) => r.key === round.key && r.slots.some((s) => s.taskId === slot.taskId),
        )
      )
        events.push({ _tag: 'ToolStart', ownerTaskId: round.ownerTaskId, slot })
  for (const n of after.nested)
    if (n.availability === 'pending' && !before.nested.some((old) => old.taskId === n.taskId))
      events.push({ _tag: 'NestedStart', nested: n })
  for (const node of after.graph.tasks)
    changed(
      { _tag: 'Task', node },
      before.graph.tasks.find((old) => old.id === node.id) === undefined
        ? undefined
        : { _tag: 'Task', node: before.graph.tasks.find((old) => old.id === node.id)! },
    )
  return events
}
