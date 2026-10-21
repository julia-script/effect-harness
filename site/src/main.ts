import { Effect, Match, Option, Schema } from 'effect'
import { Command, Dom, ManagedResource, type Runtime, Subscription, Update } from 'foldkit'
import type { Document, HtmlBuilder } from 'foldkit/html'
import { load, pushUrl, UrlRequest } from 'foldkit/navigation'
import { modifyFields } from 'foldkit/struct'
import { toString as urlToString } from 'foldkit/url'
import type { Url } from 'foldkit/url'
import * as Demo from './demo/demo.ts'
import * as Demos from './demos.ts'
import * as Guides from './guides.ts'
import { footer, header } from './layout.ts'
import { Message } from './message.ts'
import { Copied, Model } from './model.ts'
import * as GuidePage from './page/guide.ts'
import * as GuidesPage from './page/guides.ts'
import * as HomePage from './page/home.ts'
import * as NotFoundPage from './page/notFound.ts'
import * as PlaygroundPage from './page/playground.ts'
import type { AppRoute } from './route.ts'
import { urlToAppRoute } from './route.ts'

export { Message, Model }

export type Services = ManagedResource.ServiceOf<typeof Demos.release.resource>

// INIT

export const init: Runtime.RoutingApplicationInit<Model, Message> = (url: Url) => ({
  model: {
    route: urlToAppRoute(url),
    url,
    copied: Copied.Nothing(),
    release: Demo.init(),
    wordsmith: Demo.init(),
  },
})

// COMMAND

const NavigateInternal = Command.define('NavigateInternal', {
  args: { url: Schema.String },
  messages: [Message.CompletedNavigateInternal],
  execute: ({ url }) => pushUrl(url).pipe(Effect.as(Message.CompletedNavigateInternal())),
})

const LoadExternal = Command.define('LoadExternal', {
  args: { href: Schema.String },
  messages: [Message.CompletedLoadExternal],
  execute: ({ href }) => load(href).pipe(Effect.as(Message.CompletedLoadExternal())),
})

const ScrollToTop = Command.define('ScrollToTop', {
  messages: [Message.CompletedScroll],
  execute: Effect.sync(() => {
    window.scrollTo({ top: 0, behavior: 'instant' })
    return Message.CompletedScroll()
  }),
})

const ScrollToAnchor = Command.define('ScrollToAnchor', {
  args: { hash: Schema.String },
  messages: [Message.CompletedScroll],
  execute: ({ hash }) =>
    Dom.scrollIntoViewAfterPaint(`#${CSS.escape(hash)}`, { block: 'start' }).pipe(
      Effect.ignore,
      Effect.as(Message.CompletedScroll()),
    ),
})

const CopyText = Command.define('CopyText', {
  args: { text: Schema.String },
  messages: [Message.SucceededCopy, Message.FailedCopy],
  execute: ({ text }) =>
    Effect.tryPromise(() => navigator.clipboard.writeText(text)).pipe(
      Effect.as(Message.SucceededCopy({ text })),
      Effect.orElseSucceed(() => Message.FailedCopy()),
    ),
})

const WaitAfterCopy = Command.define('WaitAfterCopy', {
  args: { text: Schema.String },
  messages: [Message.CompletedWaitAfterCopy],
  execute: ({ text }) =>
    Effect.sleep('2 seconds').pipe(Effect.as(Message.CompletedWaitAfterCopy({ text }))),
})

// DEMOS

const showsRelease = (route: AppRoute): boolean =>
  route._tag === 'Home' || route._tag === 'Playground'

const showsWordsmith = (route: AppRoute): boolean => route._tag === 'Playground'

const readRelease = (model: Model) =>
  showsRelease(model.route) ? Option.some(model.release) : Option.none()

const readWordsmith = (model: Model) =>
  showsWordsmith(model.route) ? Option.some(model.wordsmith) : Option.none()

const foldRelease = Update.foldChild({
  update: Demos.release.update,
  read: readRelease,
  write: (model: Model, nextRelease) => modifyFields(model, { release: () => nextRelease }),
  toParentMessage: (message) => Message.GotReleaseMessage({ message }),
})

const foldWordsmith = Update.foldChild({
  update: Demos.wordsmith.update,
  read: readWordsmith,
  write: (model: Model, nextWordsmith) => modifyFields(model, { wordsmith: () => nextWordsmith }),
  toParentMessage: (message) => Message.GotWordsmithMessage({ message }),
})

// UPDATE

type UpdateReturn = Update.Return<Model, Message, Services>

export const update = (model: Model, message: Message) =>
  Message.match<UpdateReturn>(message, {
    ClickedLink: ({ request }) =>
      UrlRequest.match<UpdateReturn>(request, {
        Internal: ({ url }) => ({ model, commands: [NavigateInternal({ url: urlToString(url) })] }),
        External: ({ href }) => ({ model, commands: [LoadExternal({ href })] }),
      }),

    ChangedUrl: ({ url }) => ({
      model: modifyFields(model, { route: () => urlToAppRoute(url), url: () => url }),
      commands: [
        Option.match(url.hash, {
          onNone: () => ScrollToTop(),
          onSome: (hash) => ScrollToAnchor({ hash }),
        }),
      ],
    }),

    CompletedNavigateInternal: () => ({ model }),
    CompletedLoadExternal: () => ({ model }),
    CompletedScroll: () => ({ model }),

    ClickedCopy: ({ text }) => ({ model, commands: [CopyText({ text })] }),

    SucceededCopy: ({ text }) => ({
      model: modifyFields(model, { copied: () => Copied.Copied({ text }) }),
      commands: [WaitAfterCopy({ text })],
    }),

    FailedCopy: () => ({ model }),

    CompletedWaitAfterCopy: ({ text }) => ({
      model: modifyFields(model, {
        copied: (copied) =>
          copied._tag === 'Copied' && copied.text === text ? Copied.Nothing() : copied,
      }),
    }),

    CompletedRenderDiagram: () => ({ model }),
    FailedRenderDiagram: () => ({ model }),

    GotReleaseMessage: ({ message }) => foldRelease(model, message),
    GotWordsmithMessage: ({ message }) => foldWordsmith(model, message),
  })

// MANAGED RESOURCE

// NOTE: both demos name their entries alike, so each is renamed for its demo.
export const managedResources = ManagedResource.aggregate(
  {
    releaseLab: ManagedResource.lift(Demos.release.managedResources)<Model, Message>({
      read: readRelease,
      toParentMessage: (message) => Message.GotReleaseMessage({ message }),
    }).lab,
  },
  {
    wordsmithLab: ManagedResource.lift(Demos.wordsmith.managedResources)<Model, Message>({
      read: readWordsmith,
      toParentMessage: (message) => Message.GotWordsmithMessage({ message }),
    }).lab,
  },
)

// SUBSCRIPTION

export const subscriptions = Subscription.aggregate(
  {
    releaseEvents: Subscription.lift(Demos.release.subscriptions)<Model, Message>({
      read: readRelease,
      toParentMessage: (message) => Message.GotReleaseMessage({ message }),
    }).events,
  },
  {
    wordsmithEvents: Subscription.lift(Demos.wordsmith.subscriptions)<Model, Message>({
      read: readWordsmith,
      toParentMessage: (message) => Message.GotWordsmithMessage({ message }),
    }).events,
  },
)

// VIEW

const titleOf = (route: AppRoute): string =>
  Match.value(route).pipe(
    Match.withReturnType<string>(),
    Match.tag('Home', () => 'effect-harness · durable AI agents on Effect'),
    Match.tag('Playground', () => 'Playground · effect-harness'),
    Match.tag('Guides', () => 'Guides · effect-harness'),
    Match.tag('Guide', ({ slug }) =>
      Option.match(Guides.find(slug), {
        onNone: () => 'Not found · effect-harness',
        onSome: (guide) => `${guide.title} · effect-harness`,
      }),
    ),
    Match.tag('NotFound', () => 'Not found · effect-harness'),
    Match.exhaustive,
  )

const pageView = (model: Model, h: HtmlBuilder<Message>) =>
  Match.value(model.route).pipe(
    Match.tag('Home', () => HomePage.view(model, h)),
    Match.tag('Playground', () => PlaygroundPage.view(model, h)),
    Match.tag('Guides', () => GuidesPage.view(h)),
    Match.tag('Guide', ({ slug }) =>
      Option.match(Guides.find(slug), {
        onNone: () => NotFoundPage.view(`/docs/${slug}`, h),
        onSome: (guide) => GuidePage.view(guide, h),
      }),
    ),
    Match.tag('NotFound', ({ path }) => NotFoundPage.view(path, h)),
    Match.exhaustive,
  )

export const view = (model: Model, h: HtmlBuilder<Message>): Document => ({
  title: titleOf(model.route),
  body: h.div(
    [h.Class('flex min-h-screen flex-col')],
    [header(model.route, h), h.main([h.Class('flex-1')], [pageView(model, h)]), footer(h)],
  ),
})
