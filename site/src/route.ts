import { pipe, Schema } from 'effect'
import { Route } from 'foldkit'
import { defineRouteUnion, literal, slash, string } from 'foldkit/route'

export const AppRoute = defineRouteUnion({
  Home: {},
  Playground: {},
  Guides: {},
  Guide: { slug: Schema.String },
  NotFound: { path: Schema.String },
})
export type AppRoute = typeof AppRoute.Type

export const homeRouter = pipe(Route.root, Route.mapTo(AppRoute.Home))

export const playgroundRouter = pipe(literal('playground'), Route.mapTo(AppRoute.Playground))

export const guidesRouter = pipe(literal('docs'), Route.mapTo(AppRoute.Guides))

export const guideRouter = pipe(literal('docs'), slash(string('slug')), Route.mapTo(AppRoute.Guide))

export const urlToAppRoute = Route.parseUrlWithFallback(
  Route.oneOf(guideRouter, guidesRouter, playgroundRouter, homeRouter),
  AppRoute.NotFound,
)
