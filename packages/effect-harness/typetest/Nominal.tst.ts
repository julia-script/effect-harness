import type * as Identity from 'effect-harness/Identity'
import * as Agent from 'effect-harness/Agent'
import { expect, test } from 'tstyche'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Stream from 'effect/Stream'
import * as Context from 'effect/Context'
import * as Env from 'effect-harness/Env'
import * as Decode from 'effect-harness/env/Decode'
import * as LineScan from 'effect-harness/env/LineScan'
import * as Output from 'effect-harness/Output'
import * as Progress from 'effect-harness/Progress'
import type * as Hook from 'effect-harness/Hook'
import type * as Image from 'effect-harness/tools/Image'
import type * as Runner from 'effect-harness/testing/Runner'
import type * as Result from 'effect/Result'

class First extends Context.Service<First, string>()('typetest/First') {}
class Second extends Context.Service<Second, string>()('typetest/Second') {}
declare const candidate: unknown
declare const binary: NativeFiles.BinaryReader
declare const narrowProgress: Progress.Progress<'error'>
declare const wideProgress: Progress.Progress<string>
declare const handlers: Hook.Handlers<First>
declare const broadHandlers: Hook.Handlers<First | Second>
declare const bytes: Image.ByteSource<'error', First>
declare const broadBytes: Image.ByteSource<string, First | Second>
declare const broadRunner: Runner.Runner<string, First | Second>
declare const narrowRunner: Runner.Runner<'error', First>

test('public factories create nominal handles, structural copies do not impersonate handles', () => {
  const plainLine = { readLine: Effect.succeed(Option.none<Env.TextLine>()) }
  expect(Env.makeTextLineReader(plainLine)).type.toBe<Env.TextLineReader>()
  expect(plainLine).type.not.toBeAssignableTo<Env.TextLineReader>()
  const plainBinary = { info: binary.info, read: binary.read, scanLines: binary.scanLines }
  expect(plainBinary).type.not.toBeAssignableTo<NativeFiles.BinaryReader>()
  expect(NativeFiles.makeBinaryReader(plainBinary)).type.toBe<NativeFiles.BinaryReader>()
  const plainDirectory = { next: () => Effect.succeed({ entries: [], done: true }) }
  expect(plainDirectory).type.not.toBeAssignableTo<NativeFiles.DirReader>()
  expect(NativeFiles.makeDirReader(plainDirectory)).type.toBe<NativeFiles.DirReader>()
  expect(Env.makeWatcher({ mode: 'native', changes: Stream.empty })).type.toBe<Env.Watcher>()
  expect({ mode: 'native', changes: Stream.empty }).type.not.toBeAssignableTo<Env.Watcher>()
  expect(Decode.make()).type.toBe<Decode.Decoder>()
  expect({ decoder: new TextDecoder(), started: false }).type.not.toBeAssignableTo<Decode.Decoder>()
  expect(LineScan.make(0)).type.toBe<Result.Result<LineScan.State, FileError.FileError>>()
  expect(Output.make()).type.toBe<Output.Buffer>()
  if (NativeFiles.isBinaryReader(candidate)) expect(candidate).type.toBe<NativeFiles.BinaryReader>()
  if (Env.isTextLineReader(candidate)) expect(candidate).type.toBe<Env.TextLineReader>()
  if (NativeFiles.isDirReader(candidate)) expect(candidate).type.toBe<NativeFiles.DirReader>()
  if (Env.isWatcher(candidate)) expect(candidate).type.toBe<Env.Watcher>()
  if (Decode.isDecoder(candidate)) expect(candidate).type.toBe<Decode.Decoder>()
  if (LineScan.isState(candidate)) expect(candidate).type.toBe<LineScan.State>()
  if (Output.isBuffer(candidate)) expect(candidate).type.toBe<Output.Buffer>()
  // effect-nit-allow P8-typetest-exact-channels: isProgress(u: unknown) deliberately narrows to Progress<unknown>; this exact guard result retains its invariant failure parameter.
  if (Progress.isProgress(candidate)) expect(candidate).type.toBe<Progress.Progress<unknown>>()
})

test('variance matches capability use: progress invariant, callback services covariant, runner contravariant', () => {
  expect(narrowProgress).type.not.toBeAssignableTo<Progress.Progress<string>>()
  expect(wideProgress).type.not.toBeAssignableTo<Progress.Progress<'error'>>()
  expect(handlers).type.toBeAssignableTo<Hook.Handlers<First | Second>>()
  expect(broadHandlers).type.not.toBeAssignableTo<Hook.Handlers<First>>()
  expect(bytes).type.toBeAssignableTo<Image.ByteSource<string, First | Second>>()
  expect(broadBytes).type.not.toBeAssignableTo<Image.ByteSource<'error', First>>()
  expect(broadRunner).type.toBeAssignableTo<Runner.Runner<'error', First>>()
  expect(narrowRunner).type.not.toBeAssignableTo<Runner.Runner<string, First | Second>>()
})

test('canonical IDs and default policy declarations retain exact nominal and native types', () => {
  expect<Identity.EntryId>().type.not.toBeAssignableTo<Identity.ConversationId>()
  expect<Identity.ConversationId>().type.not.toBeAssignableTo<Identity.EntryId>()
  expect<typeof Identity.EntryId.Encoded>().type.toBe<number>()
  expect<typeof Identity.ConversationId.Encoded>().type.toBe<number>()
  expect(Agent.defaultRetry).type.toBe<Agent.RetryPolicy>()
  expect(Agent.defaultSettings).type.toBe<Agent.Settings>()
})

import * as NativeFiles from 'effect-harness/NativeFiles'
import type * as FileError from 'effect-harness/FileError'
