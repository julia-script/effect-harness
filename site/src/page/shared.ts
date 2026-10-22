import type { Html, HtmlBuilder } from 'foldkit/html'
import { Message } from '../message.ts'
import type { Copied } from '../model.ts'

export const container = 'mx-auto w-full max-w-7xl px-4 sm:px-6'

export const eyebrow = (text: string, h: HtmlBuilder<Message>): Html =>
  h.p([h.Class('mb-3 text-xs font-semibold uppercase tracking-[0.2em] text-zinc-500')], [text])

export const sectionTitle = (text: string, h: HtmlBuilder<Message>): Html =>
  h.h2([h.Class('text-3xl font-bold tracking-tight text-white sm:text-4xl')], [text])

export const copyCommand = (command: string, copied: Copied, h: HtmlBuilder<Message>): Html => {
  const isCopied = copied._tag === 'Copied' && copied.text === command
  return h.div(
    [
      h.Class(
        'flex w-full max-w-md items-center justify-between gap-3 rounded-xl bg-black/40 py-2 pr-2 pl-4 font-mono text-sm ring-1 ring-white/10',
      ),
    ],
    [
      h.code(
        [h.Class('truncate text-zinc-200')],
        [h.span([h.Class('select-none text-zinc-500')], ['$ ']), command],
      ),
      h.button(
        [
          h.Type('button'),
          h.OnClick(Message.ClickedCopy({ text: command })),
          h.AriaLabel('Copy the install command'),
          h.Class(
            'shrink-0 rounded-lg bg-white/5 px-3 py-1 font-sans text-xs font-semibold text-zinc-300 ring-1 ring-white/10 transition hover:bg-white/10',
          ),
        ],
        [isCopied ? 'Copied' : 'Copy'],
      ),
    ],
  )
}

/** A source file, highlighted, behind a disclosure. */
export const sourcePanel = (
  label: string,
  path: string,
  source: { readonly html: string },
  h: HtmlBuilder<Message>,
): Html =>
  h.details(
    [h.Class('group mt-4 overflow-hidden rounded-xl bg-ink-900/60 ring-1 ring-white/10')],
    [
      h.summary(
        [
          h.Class(
            'flex cursor-pointer items-center justify-between gap-3 px-4 py-3 text-sm font-semibold text-zinc-200 select-none hover:bg-white/[0.03]',
          ),
        ],
        [
          h.span([], [label]),
          h.span([h.Class('font-mono text-xs font-normal text-zinc-500')], [path]),
        ],
      ),
      h.div(
        [h.Class('highlighted max-h-[36rem] overflow-auto border-t border-white/10 font-mono')],
        [h.div([h.InnerHTML(source.html)])],
      ),
    ],
  )
