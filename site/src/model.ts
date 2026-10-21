import { Schema } from 'effect'
import { defineTaggedUnion } from 'foldkit/schema'
import { Url } from 'foldkit/url'
import * as Demo from './demo/demo.ts'
import { AppRoute } from './route.ts'

/** The text last copied, while its button says so. */
export const Copied = defineTaggedUnion({
  Nothing: {},
  Copied: { text: Schema.String },
})
export type Copied = typeof Copied.Type

export const Model = Schema.Struct({
  route: AppRoute,
  url: Url,
  copied: Copied,
  release: Demo.Model,
  wordsmith: Demo.Model,
})
export type Model = typeof Model.Type
