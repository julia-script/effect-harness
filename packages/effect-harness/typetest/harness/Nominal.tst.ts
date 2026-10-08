import { expect, test } from 'tstyche'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Stream from 'effect/Stream'
import * as Context from 'effect/Context'
import * as Env from 'effect-harness/Env'
import * as Decode from 'effect-harness/env/Decode'
import * as Scanner from 'effect-harness/env/LineScan'
import * as Output from 'effect-harness/Output'
import * as Progress from 'effect-harness/Progress'
import type * as Hook from 'effect-harness/Hook'
import type * as Image from 'effect-harness/tools/Image'
import type * as Runner from 'effect-harness/testing/Runner'
import type * as Result from 'effect/Result'

class First extends Context.Service<First, string>()('typetest/First') {}
class Second extends Context.Service<Second, string>()('typetest/Second') {}
declare const candidate: unknown
declare const binary: Env.BinaryReader
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
  expect(plainBinary).type.not.toBeAssignableTo<Env.BinaryReader>()
  expect(Env.makeBinaryReader(plainBinary)).type.toBe<Env.BinaryReader>()
  const plainDirectory = { next: () => Effect.succeed({ entries: [], done: true }) }
  expect(plainDirectory).type.not.toBeAssignableTo<Env.DirReader>()
  expect(Env.makeDirReader(plainDirectory)).type.toBe<Env.DirReader>()
  expect(Env.makeWatcher({ mode: 'native', changes: Stream.empty })).type.toBe<Env.Watcher>()
  expect({ mode: 'native', changes: Stream.empty }).type.not.toBeAssignableTo<Env.Watcher>()
  expect(Decode.make()).type.toBe<Decode.Decoder>()
  expect({ decoder: new TextDecoder(), started: false }).type.not.toBeAssignableTo<Decode.Decoder>()
  expect(Scanner.make(0)).type.toBe<Result.Result<Scanner.State, Env.FileError>>()
  expect(Output.make()).type.toBe<Output.Buffer>()
  if (Env.isBinaryReader(candidate)) expect(candidate).type.toBe<Env.BinaryReader>()
  if (Env.isTextLineReader(candidate)) expect(candidate).type.toBe<Env.TextLineReader>()
  if (Env.isDirReader(candidate)) expect(candidate).type.toBe<Env.DirReader>()
  if (Env.isWatcher(candidate)) expect(candidate).type.toBe<Env.Watcher>()
  if (Decode.isDecoder(candidate)) expect(candidate).type.toBe<Decode.Decoder>()
  if (Scanner.isState(candidate)) expect(candidate).type.toBe<Scanner.State>()
  if (Output.isBuffer(candidate)) expect(candidate).type.toBe<Output.Buffer>()
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
