import { Duration, Effect, Option, Schema, Stream } from 'effect'
import { Command, Navigation, Subscription, type Update } from 'foldkit'
import type { Document, Html, HtmlBuilder } from 'foldkit/html'
import { defineMessageUnion } from 'foldkit/message'
import { UrlRequest } from 'foldkit/navigation'
import { Url, toString } from 'foldkit/url'
import * as Lab from './Lab'
import { github, pages, sourceUrl } from './Content'
import * as Markdown from './Markdown'

const Action = Schema.Literals([
  'step',
  'stop',
  'start',
  'retry',
  'fork',
  'returnToRoot',
  'abort',
  'reset',
])
export const Model = Schema.Struct({
  path: Schema.String,
  query: Schema.String,
  menu: Schema.Boolean,
  autoplay: Schema.Boolean,
  draft: Schema.String,
  inspecting: Schema.Boolean,
  busy: Schema.Boolean,
  error: Schema.String,
  notice: Schema.String,
  lab: Lab.Snapshot,
})
export type Model = typeof Model.Type
export const Message = defineMessageUnion({
  RequestedUrl: { request: UrlRequest },
  ChangedUrl: { url: Url },
  Navigated: {},
  Scrolled: {},
  ChangedSearch: { value: Schema.String },
  ToggledMenu: {},
  ChangedDraft: { value: Schema.String },
  SentMessage: {},
  SuggestedMessage: { text: Schema.String },
  ToggledInspector: {},
  ClickedAction: { action: Action },
  ToggledAutoplay: {},
  ReceivedSnapshot: { snapshot: Lab.Snapshot },
  CompletedAction: {},
  FailedAction: { error: Schema.String },
  Ticked: {},
  Advanced: {},
  ClickedCopy: { text: Schema.String },
  Copied: {},
  CopyFailed: {},
})
export type Message = typeof Message.Type
const RunAction = Command.define('RunAction', {
  args: { action: Action },
  messages: [Message.CompletedAction, Message.FailedAction],
  execute: ({ action }) =>
    Effect.gen(function* () {
      const lab = yield* Lab.Lab
      yield* lab[action]
      return Message.CompletedAction()
    }).pipe(
      Effect.catch((error) => Effect.succeed(Message.FailedAction({ error: String(error) }))),
    ),
})
const SendMessage = Command.define('SendMessage', {
  args: { text: Schema.String },
  messages: [Message.CompletedAction, Message.FailedAction],
  execute: ({ text }) =>
    Effect.gen(function* () {
      const lab = yield* Lab.Lab
      yield* lab.submit(text)
      return Message.CompletedAction()
    }).pipe(
      Effect.catch((error) => Effect.succeed(Message.FailedAction({ error: String(error) }))),
    ),
})
const ReadLab = Command.define('ReadLab', {
  messages: [Message.ReceivedSnapshot, Message.FailedAction],
  execute: Effect.gen(function* () {
    const lab = yield* Lab.Lab
    return Message.ReceivedSnapshot({ snapshot: yield* lab.read })
  }).pipe(Effect.catch((error) => Effect.succeed(Message.FailedAction({ error: String(error) })))),
})
const Advance = Command.define('Advance', {
  messages: [Message.Advanced],
  execute: Effect.gen(function* () {
    const lab = yield* Lab.Lab
    yield* lab.step
    return Message.Advanced()
  }),
})
const Navigate = Command.define('Navigate', {
  args: { request: UrlRequest },
  messages: [Message.Navigated],
  execute: ({ request }) => {
    if (request._tag === 'External')
      return Navigation.load(request.href).pipe(Effect.as(Message.Navigated()))
    const navigate = request.url.pathname.startsWith('/sources/')
      ? Navigation.load
      : Navigation.pushUrl
    return navigate(toString(request.url)).pipe(Effect.as(Message.Navigated()))
  },
})
const ScrollPage = Command.define('ScrollPage', {
  args: { hash: Schema.String },
  messages: [Message.Scrolled],
  execute: ({ hash }) =>
    Effect.promise(
      () =>
        new Promise<void>((resolve) => {
          requestAnimationFrame(() => {
            if (hash) document.getElementById(hash.replace(/^#/, ''))?.scrollIntoView()
            else {
              window.scrollTo(0, 0)
              document.getElementById('main')?.focus({ preventScroll: true })
            }
            resolve()
          })
        }),
    ).pipe(Effect.as(Message.Scrolled())),
})
const ScrollChat = Command.define('ScrollChat', {
  args: { force: Schema.Boolean },
  messages: [Message.Scrolled],
  execute: ({ force }) =>
    Effect.promise(
      () =>
        new Promise<void>((resolve) => {
          const chat = document.querySelector('.chat-transcript')
          const nearBottom =
            chat !== null && chat.scrollHeight - chat.scrollTop - chat.clientHeight < 120
          requestAnimationFrame(() => {
            if (chat !== null && (force || nearBottom)) chat.scrollTop = chat.scrollHeight
            resolve()
          })
        }),
    ).pipe(Effect.as(Message.Scrolled())),
})
const Copy = Command.define('Copy', {
  args: { text: Schema.String },
  messages: [Message.Copied, Message.CopyFailed],
  execute: ({ text }) =>
    Effect.tryPromise(() => navigator.clipboard.writeText(text)).pipe(
      Effect.as(Message.Copied()),
      Effect.orElseSucceed(() => Message.CopyFailed()),
    ),
})
export const init = (url: Url): Update.Return<Model, Message, Lab.Lab> => ({
  model: {
    path: url.pathname.replace(/\/$/, '') || '/',
    query: '',
    menu: false,
    autoplay: true,
    draft: 'Why does the login test fail?',
    inspecting: false,
    busy: false,
    error: '',
    notice: '',
    lab: Lab.empty,
  },
  commands: [ScrollPage({ hash: Option.getOrElse(url.hash, () => '') })],
})
const isLab = (path: string) => path === '/' || path.startsWith('/lab')
export const update = (model: Model, message: Message): Update.Return<Model, Message, Lab.Lab> =>
  Message.match(message, {
    RequestedUrl: ({ request }) => ({ model, commands: [Navigate({ request })] }),
    ChangedUrl: ({ url }) => ({
      model: {
        ...model,
        path: url.pathname.replace(/\/$/, '') || '/',
        menu: false,
        query: '',
        notice: '',
      },
      commands: [ScrollPage({ hash: Option.getOrElse(url.hash, () => '') })],
    }),
    Navigated: () => ({ model }),
    Scrolled: () => ({ model }),
    ChangedSearch: ({ value }) => ({ model: { ...model, query: value } }),
    ToggledMenu: () => ({ model: { ...model, menu: !model.menu } }),
    ChangedDraft: ({ value }) => ({ model: { ...model, draft: value } }),
    SuggestedMessage: ({ text }) => ({ model: { ...model, draft: text } }),
    ToggledInspector: () => ({ model: { ...model, inspecting: !model.inspecting } }),
    SentMessage: () =>
      model.busy || model.lab.answering || !model.draft.trim()
        ? { model }
        : {
            model: { ...model, busy: true, draft: '', error: '', autoplay: true },
            commands: [SendMessage({ text: model.draft.trim() })],
          },
    ClickedAction: ({ action }) =>
      model.busy
        ? { model }
        : {
            model: {
              ...model,
              busy: true,
              error: '',
              autoplay:
                action === 'stop' || action === 'abort' || action === 'reset'
                  ? false
                  : action === 'start' || model.autoplay,
              draft: action === 'reset' ? 'Why does the login test fail?' : model.draft,
            },
            commands: [RunAction({ action })],
          },
    ToggledAutoplay: () => ({ model: { ...model, autoplay: !model.autoplay } }),
    ReceivedSnapshot: ({ snapshot }) => ({
      model: { ...model, lab: snapshot },
      commands:
        snapshot.messages.length !== model.lab.messages.length ||
        snapshot.emitted !== model.lab.emitted
          ? [
              ScrollChat({
                force:
                  snapshot.messages.filter((message) => message.role === 'user').length !==
                  model.lab.messages.filter((message) => message.role === 'user').length,
              }),
            ]
          : [],
    }),
    CompletedAction: () => ({ model: { ...model, busy: false }, commands: [ReadLab()] }),
    FailedAction: ({ error }) => ({ model: { ...model, busy: false, autoplay: false, error } }),
    Ticked: () => ({
      model,
      commands: model.autoplay && !model.busy ? [ReadLab(), Advance()] : [ReadLab()],
    }),
    Advanced: () => ({ model }),
    ClickedCopy: ({ text }) => ({ model: { ...model, notice: '' }, commands: [Copy({ text })] }),
    Copied: () => ({ model: { ...model, notice: 'Copied to clipboard' } }),
    CopyFailed: () => ({
      model: { ...model, notice: 'Clipboard unavailable. Select and copy the code directly.' },
    }),
  })
export const subscriptions = Subscription.make<Model, Message, Lab.Lab>()((entry) => ({
  poll: entry(
    { active: Schema.Boolean },
    {
      modelToDependencies: (model) => ({ active: isLab(model.path) || model.lab.answering }),
      dependenciesToStream: ({ active }) =>
        active ? Stream.tick(Duration.millis(350)).pipe(Stream.map(Message.Ticked)) : Stream.empty,
    },
  ),
}))

type H = HtmlBuilder<Message>
const link = (h: H, href: string, text: string, className = '') =>
  h.a([h.Href(href), h.Class(className)], [text])
const icon = (h: H, text: string) => h.span([h.AriaHidden(true), h.Class('icon')], [text])
const groupLinks = (model: Model, h: H, group: string) => {
  const matches = pages.filter(
    (page) =>
      page.group === group &&
      `${page.title} ${page.summary} ${page.body}`
        .toLowerCase()
        .includes(model.query.toLowerCase()),
  )
  if (matches.length === 0) return h.empty
  return h.div(
    [h.Class('nav-group')],
    [
      h.h2([], [group]),
      ...matches.map((page) =>
        link(h, page.path, page.title, model.path === page.path ? 'nav-link active' : 'nav-link'),
      ),
    ],
  )
}
const sidebar = (model: Model, h: H) =>
  h.aside(
    [
      h.Class(`sidebar ${model.menu ? 'mobile-open' : ''}`),
      h.AriaLabel('Documentation navigation'),
    ],
    [
      h.div(
        [h.Class('search-wrap')],
        [
          icon(h, '⌕'),
          h.input([
            h.Type('search'),
            h.Placeholder('Find a page…'),
            h.AriaLabel('Search documentation'),
            h.Value(model.query),
            h.OnInput((value) => Message.ChangedSearch({ value })),
          ]),
        ],
      ),
      h.nav(
        [],
        [
          ...(model.query === ''
            ? [
                h.div(
                  [h.Class('nav-group')],
                  [
                    h.h2([], ['Start here']),
                    link(h, '/', 'Overview', model.path === '/' ? 'nav-link active' : 'nav-link'),
                    link(
                      h,
                      '/lab',
                      'Conversation lab',
                      model.path === '/lab' ? 'nav-link active' : 'nav-link',
                    ),
                  ],
                ),
              ]
            : []),
          ...['Tutorials', 'Guides', 'Reference', 'Explanation', 'Examples'].map((group) =>
            groupLinks(model, h, group),
          ),
          ...(model.query !== '' &&
          !pages.some((page) =>
            `${page.title} ${page.summary} ${page.body}`
              .toLowerCase()
              .includes(model.query.toLowerCase()),
          )
            ? [
                h.p(
                  [h.Class('empty-search')],
                  ['No pages found. Try “tools”, “storage”, or “recovery”.'],
                ),
              ]
            : []),
        ],
      ),
      h.div(
        [h.Class('sidebar-footer')],
        [
          h.span([h.Class('status-dot')], []),
          h.span([], ['Effect 4 · TypeScript']),
          link(h, `${github}/issues`, 'Feedback ↗'),
        ],
      ),
    ],
  )
const action = (
  h: H,
  model: Model,
  value: typeof Action.Type,
  label: string,
  disabled = false,
  primary = false,
) =>
  h.button(
    [
      h.Type('button'),
      h.Class(primary ? 'button primary' : 'button'),
      h.Disabled(disabled || model.busy),
      h.OnClick(Message.ClickedAction({ action: value })),
    ],
    [label],
  )
const labView = (model: Model, h: H) => {
  const lab = model.lab
  const lastUser = lab.messages.findLastIndex((message) => message.role === 'user')
  const turn = lab.messages.slice(lastUser)
  const hasTool = turn.some((message) => message.role === 'tool')
  const hasAnswer = turn.some((message) => message.role === 'assistant')
  const paused = lab.answering && (!model.autoplay || !lab.running)
  let activity = 'Thinking…'
  if (lab.pending === 'Tool result') activity = 'Reading login.test.ts…'
  if (lab.emitted) activity = 'Writing a reply…'
  if (paused)
    activity = lab.running
      ? 'Paused — continue below when you’re ready.'
      : 'Runtime stopped — your saved conversation is still here.'
  const bubble = (role: 'user' | 'assistant' | 'tool', text: string, key: string) => {
    if (role === 'tool')
      return h.div(
        [h.Class('chat-tool'), h.Key(key)],
        [
          h.span([h.AriaHidden(true)], ['✓']),
          h.div([], [h.strong([], ['Read login.test.ts']), h.p([], [text])]),
        ],
      )
    return h.div(
      [h.Class(`chat-message ${role}`), h.Key(key)],
      [
        h.span([h.Class('chat-speaker')], [role === 'user' ? 'You' : 'Assistant']),
        h.div([h.Class('chat-bubble')], [text]),
      ],
    )
  }
  const step = (
    number: string,
    title: string,
    description: string,
    done: boolean,
    current: boolean,
  ) =>
    h.li(
      [h.Class(`trace-step ${done ? 'done' : ''} ${current ? 'current' : ''}`)],
      [
        h.span([h.Class('trace-number'), h.AriaHidden(true)], [done ? '✓' : number]),
        h.div([], [h.strong([], [title]), h.p([], [description])]),
      ],
    )
  return h.section(
    [h.Class('lab'), h.AriaLabel('Interactive conversation lab')],
    [
      h.div(
        [h.Class('lab-heading')],
        [
          h.div(
            [h.Class('chat-title')],
            [
              h.span([h.Class('status-dot')], []),
              h.strong([], ['Test helper']),
              h.span([h.Class('lab-label')], ['Scripted demo · no API key']),
            ],
          ),
          action(h, model, 'reset', 'New chat', lab.messages.length === 0),
        ],
      ),
      h.div(
        [h.Class('chat-transcript'), h.Role('log'), h.AriaLabel('Chat messages')],
        [
          ...(lab.messages.length === 0
            ? [
                h.div(
                  [h.Class('chat-welcome')],
                  [
                    h.span([h.Class('chat-welcome-icon'), h.AriaHidden(true)], ['↳']),
                    h.h3([], ['Let’s debug a test together.']),
                    h.p(
                      [],
                      [
                        'A login test is failing. Ask the assistant to take a look, just like you would in a coding chat.',
                      ],
                    ),
                  ],
                ),
              ]
            : lab.messages.map((message) => bubble(message.role, message.text, message.id))),
          ...(lab.answering && lab.emitted ? [bubble('assistant', lab.emitted, 'streaming')] : []),
          ...(lab.answering
            ? [
                h.p(
                  [h.Class('chat-activity')],
                  [
                    h.span([h.Class(paused ? 'paused-dot' : 'status-dot'), h.AriaHidden(true)], []),
                    activity,
                  ],
                ),
              ]
            : []),
          ...(lab.status === 'InputUnanswered'
            ? [
                h.p(
                  [h.Class('chat-activity')],
                  ['Reply stopped. Send another message to continue.'],
                ),
              ]
            : []),
        ],
      ),
      ...(lab.answering
        ? [
            h.div(
              [h.Class('chat-playback')],
              lab.running
                ? [
                    h.button(
                      [
                        h.Type('button'),
                        h.Class('button'),
                        h.Disabled(model.busy),
                        h.OnClick(Message.ToggledAutoplay()),
                      ],
                      [model.autoplay ? 'Pause reply' : 'Continue reply'],
                    ),
                  ]
                : [action(h, model, 'start', 'Resume runtime', false, true)],
            ),
          ]
        : []),
      h.form(
        [h.Class('chat-composer'), h.OnSubmit(Message.SentMessage())],
        [
          h.label([h.For('chat-input'), h.Class('sr-only')], ['Message the test helper']),
          h.input([
            h.Id('chat-input'),
            h.Type('text'),
            h.Value(model.draft),
            h.Placeholder('Ask about the login test…'),
            h.Disabled(model.busy || lab.answering),
            h.OnInput((value) => Message.ChangedDraft({ value })),
          ]),
          h.button(
            [
              h.Type('submit'),
              h.Class('button primary'),
              h.Disabled(model.busy || lab.answering || !model.draft.trim()),
            ],
            ['Send ↑'],
          ),
        ],
      ),
      h.div(
        [h.Class('chat-suggestions')],
        [
          h.span([], ['Try asking:']),
          ...['Why does the login test fail?', 'Show me the fix'].map((text) =>
            h.button(
              [
                h.Type('button'),
                h.Disabled(model.busy || lab.answering),
                h.OnClick(Message.SuggestedMessage({ text })),
              ],
              [text],
            ),
          ),
        ],
      ),
      h.p(
        [h.Class('chat-disclaimer')],
        ['This model follows a script about one login test; it isn’t a general-purpose assistant.'],
      ),
      h.div(
        [h.Class('inspector-toggle-row')],
        [
          h.button(
            [
              h.Type('button'),
              h.Class('inspector-toggle'),
              h.AriaExpanded(model.inspecting),
              h.AriaControls('chat-inspector'),
              h.OnClick(Message.ToggledInspector()),
            ],
            [h.span([h.AriaHidden(true)], [model.inspecting ? '−' : '+']), 'Under the hood'],
          ),
          h.span([], ['How does a chat become recoverable?']),
        ],
      ),
      ...(model.inspecting
        ? [
            h.div(
              [h.Id('chat-inspector'), h.Class('chat-inspector')],
              [
                h.div(
                  [h.Class('inspector-intro')],
                  [
                    h.h3([], ['The same conversation, one layer down']),
                    h.p(
                      [],
                      [
                        'Each chat event has a stored counterpart. Follow the latest message below; pause the reply to move through it one step at a time.',
                      ],
                    ),
                  ],
                ),
                h.ol(
                  [h.Class('trace-steps'), h.AriaLabel('Latest message lifecycle')],
                  [
                    step(
                      '1',
                      'Your message is saved',
                      lastUser >= 0
                        ? `“${turn[0]?.text ?? ''}” is now in the conversation history.`
                        : 'Sending admits your message and adds it to the conversation.',
                      lastUser >= 0,
                      false,
                    ),
                    step(
                      '2',
                      'The assistant uses a tool',
                      hasTool
                        ? 'The test result is saved too. A restarted model can use that result without running the completed tool again.'
                        : 'The model asks to read login.test.ts. The harness runs the tool and saves its result.',
                      hasTool,
                      lab.answering && !hasTool,
                    ),
                    step(
                      '3',
                      'The assistant’s answer is saved',
                      hasAnswer
                        ? 'The finished reply joins the history. The chat above is reading that stored answer.'
                        : 'Words appear as the model streams. Completed text becomes an answer in the conversation history.',
                      hasAnswer,
                      lab.answering && hasTool,
                    ),
                  ],
                ),
                h.div(
                  [h.Class('lab-controls')],
                  [
                    h.button(
                      [
                        h.Type('button'),
                        h.Class('button'),
                        h.Disabled(!lab.running || !lab.answering || model.busy),
                        h.AriaPressed(String(!model.autoplay)),
                        h.OnClick(Message.ToggledAutoplay()),
                      ],
                      [model.autoplay ? 'Pause stream' : 'Play stream'],
                    ),
                    action(
                      h,
                      model,
                      'step',
                      'Next step →',
                      !lab.running || !lab.pending || model.autoplay,
                    ),
                    h.span(
                      [h.Class('muted')],
                      [lab.pending ? `Next: ${lab.pending.toLowerCase()}` : 'No step waiting'],
                    ),
                  ],
                ),
                h.div(
                  [h.Class('recovery-experiment')],
                  [
                    h.div(
                      [],
                      [
                        h.h3([], ['What if the app stops mid-reply?']),
                        h.p(
                          [],
                          [
                            'Stop the runtime while the assistant is replying, then resume it. Saved history stays here. An unfinished model response can restart; a completed tool result is reused.',
                          ],
                        ),
                      ],
                    ),
                    action(
                      h,
                      model,
                      lab.running ? 'stop' : 'start',
                      lab.running ? 'Stop runtime' : 'Resume runtime',
                      lab.messages.length === 0,
                    ),
                  ],
                ),
                h.details(
                  [h.Class('technical-details')],
                  [
                    h.summary([], ['Inspect stored data & more experiments']),
                    h.div(
                      [h.Class('lab-stats')],
                      [
                        h.span([], [h.strong([], [String(lab.requests)]), ' model requests']),
                        h.span([], [h.strong([], [String(lab.toolCalls)]), ' tool executions']),
                        h.span([], [h.strong([], [String(lab.submissions)]), ' submissions']),
                        h.span([], [`Branch: ${lab.branch}`]),
                      ],
                    ),
                    h.div(
                      [h.Class('lab-columns')],
                      [
                        h.div(
                          [h.Class('stream-pane')],
                          [
                            h.h4([], ['Stream & checkpoint']),
                            h.p([], ['The provider’s latest output:']),
                            h.pre([], [lab.emitted || 'No text yet.']),
                            h.p([], ['Last committed partial:']),
                            h.pre([], [lab.partial || 'No partial response stored.']),
                          ],
                        ),
                        h.div(
                          [h.Class('journal-pane')],
                          [
                            h.h4([], [`Stored entries · commit ${lab.commit}`]),
                            h.ol(
                              [h.Class('journal')],
                              lab.entries.map((entry) =>
                                h.li(
                                  [],
                                  [
                                    h.span([h.Class('entry-id mono')], [`#${entry.id}`]),
                                    h.div([], [h.strong([], [entry.kind]), h.p([], [entry.text])]),
                                  ],
                                ),
                              ),
                            ),
                          ],
                        ),
                      ],
                    ),
                    h.div(
                      [h.Class('lab-experiments')],
                      [
                        h.div(
                          [],
                          [
                            h.h3([], ['Send the same request twice']),
                            h.p(
                              [],
                              [
                                'A repeated request ID returns the same submission. It does not add another user message.',
                              ],
                            ),
                            action(
                              h,
                              model,
                              'retry',
                              'Retry request',
                              !lab.running || !lab.canRetry,
                            ),
                            ...(lab.reused
                              ? [
                                  h.p(
                                    [h.Class('success'), h.Role('status')],
                                    ['✓ Same submission returned; no duplicate message.'],
                                  ),
                                ]
                              : []),
                          ],
                        ),
                        h.div(
                          [],
                          [
                            h.h3([], ['Take the conversation another way']),
                            h.p(
                              [],
                              [
                                'Fork a finished answer, then send a follow-up in the chat. The original branch stays as it was.',
                              ],
                            ),
                            action(
                              h,
                              model,
                              'fork',
                              'Fork conversation',
                              !lab.running || !lab.canFork,
                            ),
                            ...(lab.branch !== 'root'
                              ? [
                                  action(
                                    h,
                                    model,
                                    'returnToRoot',
                                    'Return to original',
                                    !lab.running || lab.tasks.length > 0,
                                  ),
                                ]
                              : []),
                          ],
                        ),
                      ],
                    ),
                    h.div(
                      [h.Class('task-strip')],
                      [
                        h.span(
                          [],
                          [`${lab.status} · ${lab.running ? 'runtime open' : 'runtime closed'}`],
                        ),
                        ...lab.tasks.map((task) => h.code([], [`${task.kind} · ${task.status}`])),
                        action(h, model, 'abort', 'Cancel reply', !lab.running || !lab.answering),
                      ],
                    ),
                  ],
                ),
                h.p(
                  [h.Class('lab-footnote')],
                  [
                    'This is the real harness with a controlled Effect AI stream and memory storage. Stopping the runtime keeps storage alive in this page. Refreshing the page clears it.',
                  ],
                ),
              ],
            ),
          ]
        : []),
      ...(model.error ? [h.p([h.Role('alert'), h.Class('error')], [model.error])] : []),
    ],
  )
}
const overview = (model: Model, h: H) => [
  h.div([h.Class('eyebrow breadcrumb')], ['DOCUMENTATION / OVERVIEW']),
  h.div(
    [h.Class('hero')],
    [
      h.div([h.Class('version-label')], ['effect-harness ', h.span([], ['v0.5.0'])]),
      h.h1([], ['AI conversations,', h.br([]), h.span([], ['with a memory.'])]),
      h.p(
        [h.Class('lead')],
        [
          'Build durable AI applications with Effect. Keep conversation history, tool execution, and application state together—even when the runtime restarts.',
        ],
      ),
      h.div(
        [h.Class('hero-actions')],
        [
          link(
            h,
            '/tutorials/first-conversation',
            'Build your first conversation →',
            'button primary',
          ),
          link(h, '/explanation/composition', 'Explore the architecture', 'text-link'),
        ],
      ),
      h.div(
        [h.Class('install')],
        [
          h.span([h.Class('muted')], ['$']),
          h.code([], ['bun add effect-harness effect']),
          h.button(
            [
              h.Type('button'),
              h.OnClick(Message.ClickedCopy({ text: 'bun add effect-harness effect' })),
              h.AriaLabel('Copy install command'),
            ],
            ['Copy'],
          ),
        ],
      ),
    ],
  ),
  h.div(
    [h.Class('section-heading')],
    [
      h.div(
        [],
        [
          h.h2([], ['See what the harness does']),
          h.p([], ['Start with a chat. Then peek at what makes it work.']),
        ],
      ),
      link(h, '/lab', 'Open the full lab ↗', 'text-link'),
    ],
  ),
  labView(model, h),
  h.div(
    [h.Class('section-heading lower')],
    [
      h.div(
        [],
        [
          h.h2([], ['Find your next step']),
          h.p([], ['A working example, a practical answer, or a closer look at the design.']),
        ],
      ),
    ],
  ),
  h.div(
    [h.Class('path-grid')],
    [
      [
        '/examples',
        '01',
        'Start with a working program',
        'Run offline examples for conversations, forks, recovery, and documents.',
      ],
      [
        '/guides/tools',
        '02',
        'Give your agent tools',
        'Declare schema-backed tools and supply handlers through Effect Layers.',
      ],
      [
        '/guides/persistence',
        '03',
        'Keep application state',
        'Store typed documents alongside the history that produced them.',
      ],
      [
        '/explanation/recovery',
        '04',
        'Understand recovery',
        'Learn what repeats after interruption and where the guarantees end.',
      ],
    ].map(([href, number, title, description]) =>
      h.a(
        [h.Href(href ?? '/'), h.Class('path-card')],
        [
          h.span([h.Class('card-number')], [number ?? '']),
          h.h3([], [title ?? '', h.span([], ['↗'])]),
          h.p([], [description ?? '']),
        ],
      ),
    ),
  ),
]
export const view = (model: Model, h: H): Document => {
  const page = pages.find((item) => item.path === model.path)
  const labPage = model.path === '/lab'
  let content: ReadonlyArray<Html | string>
  if (model.path === '/') content = overview(model, h)
  else if (labPage)
    content = [
      h.div([h.Class('eyebrow breadcrumb')], ['EXPLORE / INTERACTIVE DEMO']),
      h.h1([h.Class('page-title')], ['Conversation lab']),
      h.p([h.Class('lead')], ['A familiar chat, with the machinery in view.']),
      h.p(
        [h.Class('intro')],
        [
          'Ask about a failing login test. The assistant reads a test fixture and streams a reply. Open “Under the hood” whenever you want to connect what you see to what the harness stores.',
        ],
      ),
      labView(model, h),
      h.div(
        [h.Class('callout')],
        [
          h.strong([], ['What this demo proves']),
          h.p(
            [],
            [
              'The UI reads Conversation.view from the actual local runtime. The stream is a fixed fixture, and inspect_test is a safe read-only tool. Stopping closes its Effect scope; reopening reuses the same in-memory store. This demonstrates runtime recovery, not persistence across a browser reload or a machine failure.',
            ],
          ),
          link(h, '/explanation/recovery', 'Read the recovery boundaries →'),
        ],
      ),
      link(h, sourceUrl('apps/docs/src/Lab.ts'), 'View the demo implementation ↗', 'text-link'),
    ]
  else if (page)
    content = [
      h.div([h.Class('eyebrow breadcrumb')], [`DOCUMENTATION / ${page.group.toUpperCase()}`]),
      h.h1([h.Class('page-title')], [page.title]),
      h.p([h.Class('lead')], [page.summary]),
      h.div(
        [h.Class('prose')],
        Markdown.render(page.body, page.source, h, (text) => Message.ClickedCopy({ text })),
      ),
      h.div(
        [h.Class('page-bottom')],
        [
          link(h, sourceUrl(page.source), 'View page source ↗'),
          link(h, '/lab', 'Explore in the lab →'),
        ],
      ),
    ]
  else
    content = [
      h.div([h.Class('eyebrow breadcrumb')], ['404']),
      h.h1([h.Class('page-title')], ['Page not found']),
      h.p([h.Class('lead')], ['This page is not part of the documentation.']),
      link(h, '/', 'Back to the overview →', 'button primary'),
    ]
  return {
    title: `${page?.title ?? (labPage ? 'Conversation lab' : 'Overview')} · Effect Harness`,
    body: h.div(
      [],
      [
        h.a([h.Href('#main'), h.Class('skip-link')], ['Skip to content']),
        h.header(
          [h.Class('header')],
          [
            link(h, '/', 'H', 'brand-mark'),
            link(h, '/', 'effect / harness', 'brand'),
            h.span([h.Class('header-divider')], []),
            h.span([h.Class('header-docs')], ['Docs']),
            h.div(
              [h.Class('header-right')],
              [
                link(h, 'https://www.npmjs.com/package/effect-harness', 'npm ↗'),
                link(h, github, 'GitHub ↗'),
                h.button(
                  [
                    h.Class('menu-button button'),
                    h.Type('button'),
                    h.AriaExpanded(model.menu),
                    h.OnClick(Message.ToggledMenu()),
                  ],
                  ['Menu'],
                ),
              ],
            ),
          ],
        ),
        sidebar(model, h),
        h.main(
          [h.Id('main'), h.Class(`main ${page ? 'document-main' : ''}`), h.Tabindex(-1)],
          [
            h.div([h.Class('content')], content),
            h.footer(
              [h.Class('footer')],
              [
                h.span([], ['Effect Harness']),
                h.span([], ['Built with Foldkit · Powered by Effect']),
              ],
            ),
          ],
        ),
        ...(page
          ? [
              h.aside(
                [h.Class('toc'), h.AriaLabel('On this page')],
                [
                  h.h2([], ['ON THIS PAGE']),
                  ...Markdown.headings(page.body).map((heading) =>
                    link(h, `${page.path}#${heading.id}`, heading.title),
                  ),
                  h.div(
                    [h.Class('toc-source')],
                    [link(h, sourceUrl(page.source), 'Page source ↗')],
                  ),
                ],
              ),
            ]
          : []),
        ...(model.notice ? [h.div([h.Class('toast'), h.Role('status')], [model.notice])] : []),
      ],
    ),
  }
}
