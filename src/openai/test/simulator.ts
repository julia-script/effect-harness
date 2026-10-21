/**
 * Test support: a simulated model behind an OpenAI-compatible Responses API on a local port,
 * which workerd (and containers) reach. Unlike `support.ts`, it answers from what each request
 * holds: a `Brain` reads the request's user turn and tool results, and replies with text, tool
 * calls, or both. Replies report usage measured from the request's size, as a real provider
 * would, so the harness's compaction thresholds see a context that grows turn after turn.
 *
 * A request is a summarization when its instructions are the compaction prompt; the default
 * brain then answers with every `FACT:` line it reads, so a test can check what a summary kept.
 */
import type { Scope } from 'effect'
import { Effect } from 'effect'

export interface Call {
  readonly name: string
  readonly arguments: Record<string, unknown>
}

export interface Reply {
  readonly text?: string
  readonly calls?: ReadonlyArray<Call>
  /** Wait this long before streaming, as a slow model would. */
  readonly delayMs?: number
  /** Fail the request with this HTTP status instead, as an overloaded provider would. */
  readonly status?: number
}

/** One item of the request's input, simplified. */
export type Item =
  | { readonly type: 'user'; readonly text: string }
  | { readonly type: 'assistant'; readonly text: string }
  | {
      readonly type: 'call'
      readonly id: string
      readonly name: string
      readonly arguments: string
    }
  | { readonly type: 'output'; readonly id: string; readonly output: string }

export interface Request {
  readonly instructions: string
  readonly items: ReadonlyArray<Item>
  readonly tools: ReadonlyArray<string>
  readonly summarizing: boolean
  /** About a quarter of the request's characters, as real tokenizers count English. */
  readonly inputTokens: number
  /** The text of the newest user item. */
  readonly user: string
  /** The tool rounds since the newest user item: each round's outputs, by call name. */
  readonly rounds: ReadonlyArray<ReadonlyArray<{ readonly name: string; readonly output: string }>>
}

export type Brain = (request: Request) => Reply | Promise<Reply>

export interface Simulator {
  readonly url: string
  /** Every request so far, oldest first. */
  readonly requests: ReadonlyArray<Request>
  /** Hold the next requests matching `when` until `release`; resolves when one arrives. */
  readonly hold: (when?: (request: Request) => boolean) => Promise<Request>
  readonly release: () => void
  readonly setBrain: (brain: Brain) => void
}

const SUMMARIZER = 'You are a context summarization assistant'

const textOf = (content: unknown): string => {
  if (typeof content === 'string') return content
  if (!Array.isArray(content)) return ''
  return content
    .map((part: { text?: unknown }) => (typeof part.text === 'string' ? part.text : ''))
    .join('')
}

const parseItems = (input: ReadonlyArray<Record<string, unknown>>): Array<Item> =>
  input.flatMap((item): Array<Item> => {
    if (item['type'] === 'function_call')
      return [
        {
          type: 'call',
          id: String(item['call_id']),
          name: String(item['name']),
          arguments: String(item['arguments']),
        },
      ]
    if (item['type'] === 'function_call_output')
      return [
        {
          type: 'output',
          id: String(item['call_id']),
          output: typeof item['output'] === 'string' ? item['output'] : textOf(item['output']),
        },
      ]
    if (item['role'] === 'user') return [{ type: 'user', text: textOf(item['content']) }]
    if (item['role'] === 'assistant') return [{ type: 'assistant', text: textOf(item['content']) }]
    return []
  })

const toRequest = (body: Record<string, unknown>): Request => {
  const input = Array.isArray(body['input']) ? body['input'] : []
  const items = parseItems(input)
  const instructions = typeof body['instructions'] === 'string' ? body['instructions'] : ''
  const systemText = input
    .filter(
      (item: Record<string, unknown>) => item['role'] === 'system' || item['role'] === 'developer',
    )
    .map((item: Record<string, unknown>) => textOf(item['content']))
    .join('\n')
  const lastUser = items.findLastIndex((item) => item.type === 'user')
  const names = new Map<string, string>()
  const rounds: Array<Array<{ name: string; output: string }>> = []
  let previous: Item['type'] | undefined
  for (const item of items.slice(lastUser + 1)) {
    if (item.type === 'call') {
      names.set(item.id, item.name)
      if (previous !== 'call') rounds.push([])
    }
    if (item.type === 'output')
      rounds.at(-1)?.push({ name: names.get(item.id) ?? '', output: item.output })
    previous = item.type
  }
  const user = items[lastUser]
  const tools = Array.isArray(body['tools'])
    ? body['tools'].map((tool: { name?: unknown }) => String(tool.name))
    : []
  return {
    instructions: `${instructions}${systemText}`,
    items,
    tools,
    summarizing: `${instructions}${systemText}`.includes(SUMMARIZER),
    inputTokens: Math.ceil(JSON.stringify(body).length / 4),
    user: user?.type === 'user' ? user.text : '',
    rounds,
  }
}

/** Summaries keep every `FACT:` line of the transcript, and any earlier summary's. */
export const summarize = (request: Request): Reply => {
  const facts = new Set<string>()
  for (const item of request.items)
    if (item.type !== 'call')
      for (const match of ('text' in item ? item.text : item.output).matchAll(/FACT: [^\n"\\<]+/gu))
        facts.add(match[0].trim())
  return { text: `## Goal\nContinue the session.\n\n## Facts\n${[...facts].join('\n')}` }
}

let ids = 0
const id = (prefix: string) => `${prefix}_${(ids++).toString(36)}`

/** The Responses API events of one reply. */
const stream = (reply: Reply, inputTokens: number) => {
  const responseId = id('resp')
  const base = {
    id: responseId,
    object: 'response',
    created_at: 1,
    model: 'test-model',
    output: [] as Array<unknown>,
    metadata: null,
    temperature: null,
    top_p: null,
    tools: [],
    tool_choice: 'auto',
    error: null,
    incomplete_details: null,
    instructions: null,
    parallel_tool_calls: true,
  }
  const events: Array<Record<string, unknown>> = []
  let sequence = 0
  const push = (event: Record<string, unknown>) =>
    events.push({ ...event, sequence_number: sequence++ })
  push({ type: 'response.created', response: { ...base, status: 'in_progress' } })
  const output: Array<Record<string, unknown>> = []
  let index = 0
  if (reply.text !== undefined && reply.text !== '') {
    const messageId = id('msg')
    const message = (status: string, text: string) => ({
      type: 'message',
      id: messageId,
      role: 'assistant',
      status,
      content: text === '' ? [] : [{ type: 'output_text', text, annotations: [] }],
    })
    push({
      type: 'response.output_item.added',
      output_index: index,
      item: message('in_progress', ''),
    })
    // Stream in chunks, as a model does, so partial commits happen.
    const size = Math.max(1, Math.ceil(reply.text.length / 16))
    for (let at = 0; at < reply.text.length; at += size)
      push({
        type: 'response.output_text.delta',
        item_id: messageId,
        output_index: index,
        content_index: 0,
        delta: reply.text.slice(at, at + size),
        logprobs: [],
      })
    const done = message('completed', reply.text)
    push({ type: 'response.output_item.done', output_index: index, item: done })
    output.push(done)
    index++
  }
  for (const call of reply.calls ?? []) {
    const itemId = id('fc')
    const callId = id('call')
    const args = JSON.stringify(call.arguments)
    const item = (status: string, value: string) => ({
      type: 'function_call',
      id: itemId,
      call_id: callId,
      name: call.name,
      arguments: value,
      status,
    })
    push({ type: 'response.output_item.added', output_index: index, item: item('in_progress', '') })
    push({
      type: 'response.function_call_arguments.delta',
      item_id: itemId,
      output_index: index,
      delta: args,
    })
    push({
      type: 'response.function_call_arguments.done',
      item_id: itemId,
      output_index: index,
      arguments: args,
    })
    const done = item('completed', args)
    push({ type: 'response.output_item.done', output_index: index, item: done })
    output.push(done)
    index++
  }
  const outputTokens = Math.ceil(JSON.stringify(output).length / 4)
  push({
    type: 'response.completed',
    response: {
      ...base,
      status: 'completed',
      output,
      usage: {
        input_tokens: inputTokens,
        output_tokens: outputTokens,
        total_tokens: inputTokens + outputTokens,
        input_tokens_details: { cached_tokens: 0 },
        output_tokens_details: { reasoning_tokens: 0 },
      },
    },
  })
  return events
}

const models = { object: 'list', data: [{ id: 'test-model', object: 'model' }] }

/** A web page for tools to fetch: about 3 KB of prose, named after `name`. */
export const page = (name: string) =>
  `# Page ${name}\n\n${Array.from(
    { length: 30 },
    (_, line) => `Line ${line} of page ${name}: the quick brown fox jumps over the lazy dog again.`,
  ).join('\n')}\n`

/** Answers with the default brain until `setBrain`: echoes the user, summarizes on request. */
export const serveSimulator = (brain?: Brain): Effect.Effect<Simulator, never, Scope.Scope> =>
  Effect.gen(function* () {
    const requests: Array<Request> = []
    const state = {
      brain: brain ?? ((request: Request): Reply => ({ text: `You said: ${request.user}` })),
      held: undefined as
        | {
            readonly when: (request: Request) => boolean
            readonly arrived: PromiseWithResolvers<Request>
            readonly open: PromiseWithResolvers<void>
          }
        | undefined,
    }
    const server = yield* Effect.acquireRelease(
      Effect.sync(() =>
        Bun.serve({
          hostname: '0.0.0.0',
          port: 0,
          idleTimeout: 0,
          fetch: async (incoming) => {
            const path = new URL(incoming.url).pathname
            if (path.endsWith('/models')) return Response.json(models)
            if (path.startsWith('/big/'))
              return new Response('x'.repeat(Number(path.slice('/big/'.length))))
            if (path.startsWith('/page/')) return new Response(page(path.slice('/page/'.length)))
            const request = toRequest((await incoming.json()) as Record<string, unknown>)
            requests.push(request)
            const held = state.held
            if (held !== undefined && held.when(request)) {
              held.arrived.resolve(request)
              await held.open.promise
            }
            const reply = request.summarizing ? summarize(request) : await state.brain(request)
            if (reply.delayMs !== undefined) await Bun.sleep(reply.delayMs)
            if (reply.status !== undefined)
              return Response.json(
                { error: { message: 'simulated failure', type: 'server_error', code: null } },
                { status: reply.status },
              )
            const body = stream(reply, request.inputTokens)
              .map((event) => `data: ${JSON.stringify(event)}\n\n`)
              .join('')
            return new Response(body, { headers: { 'content-type': 'text/event-stream' } })
          },
        }),
      ),
      (served) => Effect.promise(() => served.stop(true)),
    )
    return {
      url: `http://127.0.0.1:${server.port}`,
      requests,
      setBrain: (next) => {
        state.brain = next
      },
      hold: (when = () => true) => {
        const held = {
          when,
          arrived: Promise.withResolvers<Request>(),
          open: Promise.withResolvers<void>(),
        }
        state.held = held
        return held.arrived.promise
      },
      release: () => {
        state.held?.open.resolve()
        state.held = undefined
      },
    }
  })
