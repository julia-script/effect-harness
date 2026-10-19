import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as Scope from 'effect/Scope'
import type * as PlatformError from 'effect/PlatformError'
import type { Failure as ExecutionFailure } from 'effect-harness/ExecutionError'
import type { RegistryError } from 'effect-harness/RegistryError'
import type { ImagePreparationError } from 'effect-harness/ImagePreparationError'
import type { ToolExecution } from 'effect-harness/ToolExecution'
import * as Files from 'effect-harness/testing/FileSystem'
import * as Processes from 'effect-harness/testing/Process'
import * as Images from 'effect-harness/testing/Images'
import * as Isolation from 'effect-harness/testing/ToolkitIsolation'

class Config extends Context.Service<Config, string>()('test/service-conformance/Config') {}
declare const files: Effect.Effect<
  Files.FileSystemFixture<'cleanup', Config>,
  'factory',
  Config | Scope.Scope
>
declare const processes: Effect.Effect<
  Processes.ProcessFixture<'cleanup', Config>,
  'factory',
  Config | Scope.Scope
>
declare const images: Effect.Effect<
  Images.ImagesFixture<'inspect', Config>,
  'factory',
  Config | Scope.Scope
>
declare const isolation: Effect.Effect<
  Isolation.ToolkitIsolationFixture<'acquire', Config>,
  'factory',
  Config | Scope.Scope
>
const fileCases = Files.fileSystemConformance({ make: files })
const processCases = Processes.processConformance({ make: processes })
const imageCases = Images.imagesConformance({ make: images })
const isolationCases = Isolation.toolkitIsolationConformance({ make: isolation })

test('public cases preserve factory cleanup codec and adapter R/E while owning Scope', () => {
  expect(fileCases[0]!.run).type.toBe<
    Effect.Effect<
      void,
      'factory' | 'cleanup' | Files.FileSystemError | Files.ServiceConformanceError,
      Config
    >
  >()
  expect(processCases[0]!.run).type.toBe<
    Effect.Effect<
      void,
      'factory' | 'cleanup' | Processes.ProcessError | Files.ServiceConformanceError,
      Config
    >
  >()
  expect(imageCases[0]!.run).type.toBe<
    Effect.Effect<
      void,
      'factory' | 'inspect' | ImagePreparationError | Files.ServiceConformanceError,
      Config
    >
  >()
  expect(isolationCases[0]!.run).type.toBe<
    Effect.Effect<
      void,
      'factory' | 'acquire' | ExecutionFailure | RegistryError | Files.ServiceConformanceError,
      Config | ToolExecution
    >
  >()
})

declare const fixture: Files.FileSystemFixture
test('a pure fixture introduces no services and typed cleanup stays visible', () => {
  const cases = Files.fileSystemConformance({ make: Effect.succeed(fixture) })
  expect(cases[0]!.run).type.toBe<
    Effect.Effect<void, Files.FileSystemError | Files.ServiceConformanceError>
  >()
  const withCleanup = Files.fileSystemConformance({
    make: Effect.succeed({
      ...fixture,
      cleanup: Effect.fail('cleanup failed' as const),
    }),
  })
  expect(withCleanup[0]!.run).type.toBe<
    Effect.Effect<void, 'cleanup failed' | Files.FileSystemError | Files.ServiceConformanceError>
  >()
  expect<PlatformError.PlatformError>().type.toBeAssignableTo<Files.FileSystemError>()
})
