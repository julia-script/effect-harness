import { Schema } from 'effect'
import { defineMessageUnion } from 'foldkit/message'
import { UrlRequest } from 'foldkit/navigation'
import { Url } from 'foldkit/url'
import * as Demo from './demo/demo.ts'

export const Message = defineMessageUnion({
  ClickedLink: { request: UrlRequest },
  ChangedUrl: { url: Url },
  CompletedNavigateInternal: {},
  CompletedLoadExternal: {},
  CompletedScroll: {},
  ClickedCopy: { text: Schema.String },
  SucceededCopy: { text: Schema.String },
  FailedCopy: {},
  CompletedWaitAfterCopy: { text: Schema.String },
  CompletedRenderDiagram: {},
  FailedRenderDiagram: { reason: Schema.String },
  GotReleaseMessage: { message: Demo.Message },
  GotWordsmithMessage: { message: Demo.Message },
})
export type Message = typeof Message.Type
