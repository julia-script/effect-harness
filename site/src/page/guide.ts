import * as Markdown from '@foldkit/markdown'
import { Array, Option, pipe } from 'effect'
import type { Html, HtmlBuilder } from 'foldkit/html'
import type { Guide } from '../guides.ts'
import { guides } from '../guides.ts'
import { guideViews, REPOSITORY, sectionsOf, withoutContents } from '../markdown/views.ts'
import type { Message } from '../message.ts'
import { guideRouter } from '../route.ts'

const sidebar = (current: Guide, h: HtmlBuilder<Message>): Html =>
  h.nav(
    [h.AriaLabel('Guides'), h.Class('flex flex-col gap-0.5')],
    [
      h.p(
        [h.Class('mb-2 px-3 text-xs font-semibold uppercase tracking-wider text-zinc-500')],
        ['Guides'],
      ),
      ...Array.map(guides, (guide) => {
        const isCurrent = guide.slug === current.slug
        return h.a(
          [
            h.Href(guideRouter({ slug: guide.slug })),
            ...(isCurrent ? [h.AriaCurrent('page')] : []),
            h.Class(
              `flex items-center gap-2.5 rounded-lg px-3 py-1.5 text-sm transition ${
                isCurrent
                  ? 'bg-violet-accent/10 font-semibold text-white ring-1 ring-violet-accent/30'
                  : 'text-zinc-400 hover:bg-white/5 hover:text-white'
              }`,
            ),
          ],
          [h.span([h.Class('w-5 text-center')], [guide.icon]), guide.title],
        )
      }),
    ],
  )

const tableOfContents = (guide: Guide, h: HtmlBuilder<Message>): Html =>
  Array.match(sectionsOf(guide), {
    onEmpty: () => h.empty,
    onNonEmpty: (sections) =>
      h.nav(
        [h.AriaLabel('On this page'), h.Class('flex flex-col gap-1 text-sm')],
        [
          h.p(
            [h.Class('mb-2 text-xs font-semibold uppercase tracking-wider text-zinc-500')],
            ['On this page'],
          ),
          ...Array.map(sections, ({ id, title }) =>
            h.a(
              [
                h.Href(`#${id}`),
                h.Class(
                  'border-l border-white/10 py-1 pl-3 text-zinc-400 transition hover:border-violet-accent hover:text-white',
                ),
              ],
              [title],
            ),
          ),
        ],
      ),
  })

const neighbour = (
  maybeGuide: Option.Option<Guide>,
  label: string,
  alignClass: string,
  h: HtmlBuilder<Message>,
): Html =>
  Option.match(maybeGuide, {
    onNone: () => h.div([]),
    onSome: (guide) =>
      h.a(
        [
          h.Href(guideRouter({ slug: guide.slug })),
          h.Class(
            `flex flex-col gap-1 rounded-xl bg-white/[0.03] p-4 ring-1 ring-white/10 transition hover:ring-violet-accent/40 ${alignClass}`,
          ),
        ],
        [
          h.span([h.Class('text-xs text-zinc-500')], [label]),
          h.span([h.Class('font-semibold text-white')], [`${guide.icon} ${guide.title}`]),
        ],
      ),
  })

const pager = (guide: Guide, h: HtmlBuilder<Message>): Html => {
  const index = Array.findFirstIndex(guides, (each) => each.slug === guide.slug)
  const at = (offset: number) =>
    pipe(
      index,
      Option.flatMap((position) => Array.get(guides, position + offset)),
    )
  return h.div(
    [h.Class('mt-16 flex flex-col gap-6 border-t border-white/10 pt-8')],
    [
      h.div(
        [h.Class('grid gap-3 sm:grid-cols-2')],
        [neighbour(at(-1), '← Previous', '', h), neighbour(at(1), 'Next →', 'sm:text-right', h)],
      ),
      h.a(
        [
          h.Href(`${REPOSITORY}/edit/main/docs/${guide.slug}.md`),
          h.Class('text-sm text-zinc-500 hover:text-white'),
        ],
        ['✎ Edit this page on GitHub'],
      ),
    ],
  )
}

const mobileNav = (guide: Guide, h: HtmlBuilder<Message>): Html =>
  h.details(
    [h.Class('mb-8 rounded-xl bg-white/[0.03] ring-1 ring-white/10 lg:hidden')],
    [
      h.summary(
        [h.Class('cursor-pointer px-4 py-3 text-sm font-semibold text-white select-none')],
        [`${guide.icon} ${guide.title}`],
      ),
      h.div([h.Class('px-2 pb-3')], [sidebar(guide, h)]),
    ],
  )

export const view = (guide: Guide, h: HtmlBuilder<Message>): Html =>
  h.div(
    [
      h.Class(
        'mx-auto grid max-w-7xl gap-10 px-4 py-12 sm:px-6 lg:grid-cols-[14rem_minmax(0,1fr)] xl:grid-cols-[14rem_minmax(0,1fr)_13rem]',
      ),
    ],
    [
      h.aside(
        [h.Class('hidden lg:block')],
        [h.div([h.Class('sticky top-24')], [sidebar(guide, h)])],
      ),
      h.article(
        [h.Class('min-w-0')],
        [
          mobileNav(guide, h),
          ...Markdown.viewBlocks(withoutContents(guide.document), guideViews(guide, h)),
          pager(guide, h),
        ],
      ),
      h.aside(
        [h.Class('hidden xl:block')],
        [
          h.div(
            [h.Class('sticky top-24 max-h-[calc(100vh-8rem)] overflow-y-auto')],
            [tableOfContents(guide, h)],
          ),
        ],
      ),
    ],
  )
