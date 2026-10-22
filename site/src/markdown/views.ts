/**
 * How a guide renders: the site's styling for every markdown node, GitHub's alerts and heading
 * anchors, links between guides rewritten to site routes, highlighted code and live diagrams.
 */
import * as Markdown from '@foldkit/markdown'
import { Array, Match, Option, pipe, String } from 'effect'
import type { Html, HtmlBuilder } from 'foldkit/html'
import * as Diagram from '../diagram.ts'
import type { Guide } from '../guides.ts'
import type { Message } from '../message.ts'
import { islandAttributes } from './islands.ts'

export const REPOSITORY = 'https://github.com/julia-script/effect-harness'

// TEXT

export const inlineText = (content: ReadonlyArray<Markdown.Inline>): string =>
  Array.join(
    Array.map(content, (inline) =>
      Match.value(inline).pipe(
        Match.withReturnType<string>(),
        Match.tag('Text', 'InlineCode', ({ value }) => value),
        Match.tag('HardBreak', () => ' '),
        Match.tag('Image', ({ alt }) => alt),
        Match.tag('Emphasis', 'Strong', 'Strikethrough', 'Link', ({ content }) =>
          inlineText(content),
        ),
        Match.exhaustive,
      ),
    ),
    '',
  )

/** GitHub's heading anchors, so links written for GitHub land in the same place. */
export const slugOf = (text: string): string =>
  pipe(
    String.toLowerCase(String.trim(text)),
    String.replace(/[^\p{L}\p{M}\p{N}\p{Pc} -]/gu, ''),
    String.replaceAll(' ', '-'),
  )

// TABLE OF CONTENTS

export interface Section {
  readonly id: string
  readonly title: string
}

export const sectionsOf = (guide: Guide): ReadonlyArray<Section> =>
  Array.flatMap(guide.document.blocks, (block) =>
    block._tag === 'Heading' && block.level === 2
      ? [{ id: slugOf(inlineText(block.content)), title: inlineText(block.content) }]
      : [],
  )

/** The guides open with a list of links to their own sections; the site shows its own. */
const isLocalContents = (block: Markdown.Block): boolean =>
  block._tag === 'List' &&
  Array.every(block.items, (item) =>
    Array.every(item.blocks, (inner) =>
      Array.every(
        inner._tag === 'Paragraph' ? inner.content : [],
        (inline) => inline._tag === 'Link' && String.startsWith('#')(inline.url),
      ),
    ),
  )

export const withoutContents = (
  document: Markdown.MarkdownDocument,
): Markdown.MarkdownDocument => ({
  ...document,
  blocks: Array.filter(document.blocks, (block) => !isLocalContents(block)),
})

// LINKS

const GUIDE_LINK = /^([a-z-]+)\.md(#.*)?$/

/** Where a link written for GitHub goes on the site. */
export const hrefOf = (url: string): string =>
  Match.value(url).pipe(
    Match.when(String.startsWith('#'), (anchor) => anchor),
    Match.when(
      (value) => GUIDE_LINK.test(value),
      (value) =>
        pipe(
          Option.fromNullishOr(GUIDE_LINK.exec(value)),
          Option.match({
            onNone: () => '/docs',
            onSome: ([, slug, anchor]) => `/docs/${slug}${anchor ?? ''}`,
          }),
        ),
    ),
    Match.when(String.startsWith('../README.md'), (value) =>
      String.replace('../README.md', REPOSITORY)(value),
    ),
    Match.when(
      String.startsWith('../'),
      (value) =>
        `${REPOSITORY}/${/\.[a-z]+(#.*)?$/.test(value) ? 'blob' : 'tree'}/main/${value.slice(3)}`,
    ),
    Match.orElse((value) => value),
  )

// ALERTS

const ALERT = /^\[!(NOTE|TIP|IMPORTANT|WARNING|CAUTION)\]\s*/

const Alert = {
  NOTE: { icon: 'ℹ️', label: 'Note', className: 'border-sky-accent/60 bg-sky-accent/5' },
  TIP: { icon: '💡', label: 'Tip', className: 'border-emerald-400/60 bg-emerald-400/5' },
  IMPORTANT: {
    icon: '❗',
    label: 'Important',
    className: 'border-violet-accent/60 bg-violet-accent/5',
  },
  WARNING: { icon: '⚠️', label: 'Warning', className: 'border-amber-400/60 bg-amber-400/5' },
  CAUTION: { icon: '🛑', label: 'Caution', className: 'border-rose-400/60 bg-rose-400/5' },
} as const

type AlertKind = keyof typeof Alert

const alertOf = (blockquote: Markdown.Blockquote): Option.Option<AlertKind> =>
  pipe(
    Array.head(blockquote.blocks),
    Option.flatMap((block) =>
      block._tag === 'Paragraph' ? Array.head(block.content) : Option.none(),
    ),
    Option.flatMap((inline) =>
      inline._tag === 'Text' ? Option.fromNullishOr(ALERT.exec(inline.value)?.[1]) : Option.none(),
    ),
    Option.filter((kind): kind is AlertKind => kind in Alert),
  )

// VIEWS

const headingClass = {
  1: 'mb-6 text-4xl font-extrabold tracking-tight text-white sm:text-5xl',
  2: 'group mt-14 mb-4 scroll-mt-24 border-t border-white/10 pt-10 text-2xl font-bold tracking-tight text-white',
  3: 'group mt-10 mb-3 scroll-mt-24 text-xl font-semibold text-white',
  4: 'group mt-8 mb-2 scroll-mt-24 text-lg font-semibold text-zinc-100',
  5: 'group mt-6 mb-2 scroll-mt-24 font-semibold text-zinc-100',
  6: 'group mt-6 mb-2 scroll-mt-24 font-semibold text-zinc-300',
} as const

const headingView = (
  heading: Markdown.Heading,
  content: Markdown.InlineContent,
  h: HtmlBuilder<Message>,
): Html => {
  const id = slugOf(inlineText(heading.content))
  const attributes = [h.Id(id), h.Class(headingClass[heading.level])]
  const anchored = [
    ...content,
    h.a(
      [
        h.Href(`#${id}`),
        h.AriaLabel('Link to this section'),
        h.Class(
          'ml-2 text-zinc-600 opacity-0 transition group-hover:opacity-100 hover:text-violet-accent',
        ),
      ],
      ['#'],
    ),
  ]
  return Match.value(heading.level).pipe(
    Match.withReturnType<Html>(),
    Match.when(1, () => h.h1([h.Class(headingClass[1])], content)),
    Match.when(2, () => h.h2(attributes, anchored)),
    Match.when(3, () => h.h3(attributes, anchored)),
    Match.when(4, () => h.h4(attributes, anchored)),
    Match.when(5, () => h.h5(attributes, anchored)),
    Match.when(6, () => h.h6(attributes, anchored)),
    Match.exhaustive,
  )
}

const codeView = (
  guide: Guide,
  block: Markdown.CodeBlock,
  occurrenceIndex: number,
  h: HtmlBuilder<Message>,
): Html =>
  Option.contains(block.maybeLanguage, 'mermaid')
    ? Diagram.view(`diagram-${guide.slug}-${occurrenceIndex}`, block.value, h)
    : pipe(
        Option.fromNullishOr(guide.code[occurrenceIndex]),
        Option.match({
          onNone: () =>
            h.pre(
              [
                h.Class(
                  'my-6 overflow-x-auto rounded-xl bg-ink-900/70 p-4 font-mono text-[13px] ring-1 ring-white/10',
                ),
              ],
              [h.code([], [block.value])],
            ),
          onSome: (html) =>
            h.div(
              [
                h.Class(
                  'highlighted relative my-6 rounded-xl bg-ink-900/70 font-mono ring-1 ring-white/10',
                ),
              ],
              [
                h.div([h.InnerHTML(html)]),
                pipe(
                  block.maybeLanguage,
                  Option.match({
                    onNone: () => h.empty,
                    onSome: (language) =>
                      h.span(
                        [
                          h.Class(
                            'pointer-events-none absolute top-2 right-3 font-sans text-[10px] font-semibold uppercase tracking-wider text-zinc-600',
                          ),
                        ],
                        [language],
                      ),
                  }),
                ),
              ],
            ),
        }),
      )

const tableCellClass = 'border-b border-white/5 px-3 py-2 align-top'

export const guideViews = (guide: Guide, h: HtmlBuilder<Message>): Markdown.ViewConfig => ({
  views: {
    Text: ({ value }) => String.replace(ALERT, '')(value),
    InlineCode: ({ value }) =>
      h.code(
        [
          h.Class(
            'rounded-md bg-white/[0.07] px-1.5 py-0.5 font-mono text-[0.85em] text-violet-200',
          ),
        ],
        [value],
      ),
    Strong: (_strong, content) => h.strong([h.Class('font-semibold text-white')], content),
    Link: ({ url }, content) =>
      h.a(
        [
          h.Href(hrefOf(url)),
          h.Class(
            'font-medium text-sky-accent underline decoration-sky-accent/30 underline-offset-4 transition hover:decoration-sky-accent',
          ),
        ],
        content,
      ),
    Heading: (heading, content) => headingView(heading, content, h),
    Paragraph: (_paragraph, content) => h.p([h.Class('my-4 leading-7 text-zinc-300')], content),
    CodeBlock: (block, occurrenceIndex) => codeView(guide, block, occurrenceIndex, h),
    List: (list, items) =>
      list.isOrdered
        ? h.ol(
            [h.Class('my-4 list-decimal space-y-2 pl-6 text-zinc-300 marker:text-zinc-500')],
            items,
          )
        : h.ul(
            [h.Class('my-4 list-disc space-y-2 pl-6 text-zinc-300 marker:text-violet-accent/70')],
            items,
          ),
    ListItem: (_item, blocks) => h.li([h.Class('pl-1 leading-7 [&>p]:my-0')], blocks),
    Blockquote: (blockquote, blocks) =>
      pipe(
        alertOf(blockquote),
        Option.match({
          onNone: () =>
            h.blockquote([h.Class('my-6 border-l-2 border-white/20 pl-4 text-zinc-400')], blocks),
          onSome: (kind) =>
            h.aside(
              [
                h.Class(
                  `my-6 rounded-r-xl border-l-4 px-5 py-3 ${Alert[kind].className} [&>p]:my-2`,
                ),
              ],
              [
                h.p(
                  [h.Class('mt-1 mb-0 text-sm font-semibold text-white')],
                  [`${Alert[kind].icon} ${Alert[kind].label}`],
                ),
                ...blocks,
              ],
            ),
        }),
      ),
    ThematicBreak: () => h.hr([h.Class('my-10 border-white/10')]),
    Table: (_table, headerRow, bodyRows) =>
      h.div(
        [h.Class('my-6 overflow-x-auto rounded-xl ring-1 ring-white/10')],
        [
          h.table(
            [h.Class('w-full border-collapse text-left text-sm')],
            [h.thead([h.Class('bg-white/[0.04]')], [headerRow]), h.tbody([], bodyRows)],
          ),
        ],
      ),
    TableRow: (_row, cells) => h.tr([h.Class('transition hover:bg-white/[0.02]')], cells),
    TableCell: (_cell, content, _alignment, isHeader) =>
      isHeader
        ? h.th([h.Class(`${tableCellClass} font-semibold text-white`)], content)
        : h.td([h.Class(`${tableCellClass} text-zinc-300`)], content),
  },
  islands: Markdown.islandsFor(islandAttributes, {
    Details: ({ summary }, content) =>
      h.details(
        [h.Class('group my-6 rounded-xl bg-white/[0.03] ring-1 ring-white/10 open:pb-2')],
        [
          h.summary(
            [h.Class('cursor-pointer px-4 py-3 font-semibold text-white select-none')],
            [summary],
          ),
          h.div([h.Class('px-4')], content),
        ],
      ),
  }),
})
