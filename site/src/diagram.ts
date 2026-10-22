/**
 * Mermaid diagrams, drawn in the browser when one scrolls into a page. Mermaid is large, so it
 * loads on first use.
 */
import { Effect, Schema } from 'effect'
import { Mount } from 'foldkit'
import type { Html, HtmlBuilder } from 'foldkit/html'
import { Message } from './message.ts'

const loadMermaid = Effect.promise(() => import('mermaid')).pipe(
  Effect.map(({ default: mermaid }) => {
    mermaid.initialize({
      startOnLoad: false,
      theme: 'base',
      fontFamily: 'Inter, ui-sans-serif, system-ui, sans-serif',
      themeVariables: {
        darkMode: true,
        background: '#0a0a0a',
        primaryColor: '#1f1f1f',
        primaryTextColor: '#e4e4e7',
        primaryBorderColor: '#52525b',
        secondaryColor: '#1f1f1f',
        tertiaryColor: '#141414',
        lineColor: '#71717a',
        clusterBkg: '#141414',
        clusterBorder: '#3f3f46',
        edgeLabelBackground: '#0a0a0a',
      },
    })
    return mermaid
  }),
)

const RenderDiagram = Mount.define('RenderDiagram', {
  args: { diagramId: Schema.String, source: Schema.String },
  messages: [Message.CompletedRenderDiagram, Message.FailedRenderDiagram],
  execute: ({ element, diagramId, source }) =>
    loadMermaid.pipe(
      Effect.flatMap((mermaid) => Effect.tryPromise(() => mermaid.render(diagramId, source))),
      Effect.tap(({ svg }) =>
        Effect.sync(() => {
          element.innerHTML = svg
        }),
      ),
      Effect.as(Message.CompletedRenderDiagram()),
      Effect.catch((error) =>
        Effect.succeed(Message.FailedRenderDiagram({ reason: `${error.message}` })),
      ),
    ),
})

// NOTE: the figure has no children of its own: Mermaid owns everything inside it.
export const view = (diagramId: string, source: string, h: HtmlBuilder<Message>): Html =>
  h.figure([
    h.Class(
      'diagram my-6 flex min-h-32 items-center justify-center overflow-x-auto rounded-xl bg-ink-900/60 p-6 ring-1 ring-white/10',
    ),
    h.AriaLabel('Diagram'),
    h.OnMount(RenderDiagram({ diagramId, source })),
  ])
