import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import type * as Path from 'effect/Path'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'
import type * as Stream from 'effect/Stream'
import * as DirectoryReader from 'effect-harness/DirectoryReader'
import * as DirectoryReaderNode from 'effect-harness/DirectoryReaderNode'
import * as DirectoryReaderBun from 'effect-harness/DirectoryReaderBun'
import * as FileWatch from 'effect-harness/FileWatch'
import * as FileWatchNode from 'effect-harness/FileWatchNode'
import * as FileWatchBun from 'effect-harness/FileWatchBun'

class Credentials extends Context.Service<Credentials, string>()('DirectoryWatch/Credentials') {}
class AcquisitionError extends Schema.TaggedError<AcquisitionError>()(
  'DirectoryWatchAcquisitionError',
  {},
) {}
declare const remote: Layer.Layer<
  DirectoryReader.Adapter | FileWatch.Adapter | Path.Path,
  AcquisitionError,
  Credentials
>
declare const reader: DirectoryReader.Reader
declare const watch: FileWatch.Watch
const options = { root: '/remote' }

test('directory and watch Layers retain backend/path requirements and acquisition failures', () => {
  const directories = DirectoryReader.layer(options)
  const watches = FileWatch.layer(options)
  expect(directories).type.toBe<
    Layer.Layer<
      DirectoryReader.DirectoryReader,
      DirectoryReader.DirectoryReaderError,
      DirectoryReader.Adapter | Path.Path
    >
  >()
  expect(watches).type.toBe<
    Layer.Layer<FileWatch.FileWatch, FileWatch.FileWatchError, FileWatch.Adapter | Path.Path>
  >()
  expect(Layer.merge(directories, watches).pipe(Layer.provide(remote))).type.toBe<
    Layer.Layer<
      DirectoryReader.DirectoryReader | FileWatch.FileWatch,
      DirectoryReader.DirectoryReaderError | FileWatch.FileWatchError | AcquisitionError,
      Credentials
    >
  >()
  expect(Effect.flatMap(DirectoryReader.DirectoryReader, (s) => s.open('dir'))).type.toBe<
    Effect.Effect<
      DirectoryReader.Reader,
      DirectoryReader.DirectoryReaderError,
      DirectoryReader.DirectoryReader | Scope.Scope
    >
  >()
  expect(Effect.flatMap(FileWatch.FileWatch, (s) => s.open([{ path: 'missing' }]))).type.toBe<
    Effect.Effect<FileWatch.Watch, FileWatch.FileWatchError, FileWatch.FileWatch | Scope.Scope>
  >()
  expect(reader.next(7)).type.toBe<
    Effect.Effect<
      { readonly entries: ReadonlyArray<DirectoryReader.Entry>; readonly done: boolean },
      DirectoryReader.DirectoryReaderError
    >
  >()
  expect(watch.events).type.toBe<Stream.Stream<FileWatch.Event, FileWatch.FileWatchError>>()
  expect(watch.close).type.toBe<Effect.Effect<void>>()
  expect(directories.pipe(Layer.provide(FileWatch.pollingAdapter(1000)))).type.not.toBeAssignableTo<
    Layer.Layer<DirectoryReader.DirectoryReader>
  >()
  expect(reader.next).type.not.toBeCallableWith('one')
})

test('explicit Node/Bun native Layers expose semantic construction errors and no implicit backend inputs', () => {
  expect(DirectoryReaderNode.layer(options)).type.toBe<
    Layer.Layer<DirectoryReader.DirectoryReader, DirectoryReader.DirectoryReaderError>
  >()
  expect(DirectoryReaderBun.layer(options)).type.toBe<
    Layer.Layer<DirectoryReader.DirectoryReader, DirectoryReader.DirectoryReaderError>
  >()
  expect(FileWatchNode.layer(options)).type.toBe<
    Layer.Layer<FileWatch.FileWatch, FileWatch.FileWatchError>
  >()
  expect(FileWatchBun.layer(options)).type.toBe<
    Layer.Layer<FileWatch.FileWatch, FileWatch.FileWatchError>
  >()
  expect(
    Effect.scoped(Effect.flatMap(DirectoryReader.DirectoryReader, (s) => s.open('.'))).pipe(
      Effect.provide(DirectoryReaderNode.layer(options)),
    ),
  ).type.toBe<Effect.Effect<DirectoryReader.Reader, DirectoryReader.DirectoryReaderError>>()
  expect(DirectoryReader.layer).type.not.toBeCallableWith({ root: 42 })
  expect(FileWatch.layer).type.not.toBeCallableWith({})
})
