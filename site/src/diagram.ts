/** A diagram, drawn as SVG at build time by `plugins/diagram`. */
import type { Html, HtmlBuilder } from 'foldkit/html'
import type { Message } from './message.ts'

export const view = (svg: string, h: HtmlBuilder<Message>): Html =>
  h.figure(
    [
      h.Class('diagram my-6 overflow-x-auto rounded-xl bg-ink-900/60 p-6 ring-1 ring-white/10'),
      h.InnerHTML(svg),
    ],
    [],
  )
