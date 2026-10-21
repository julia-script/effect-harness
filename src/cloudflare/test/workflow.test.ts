/**
 * `HarnessWorkflow` without workerd: a stand-in for Cloudflare's step runs each step's Effect at
 * once and records its name and config, and `Faux` answers.
 */
import { describe, expect, test } from 'bun:test'
import { WorkflowStep } from 'alchemy/Cloudflare/Workflows'
import type { WorkflowStepConfig, WorkflowTaskOptions } from 'alchemy/Cloudflare/Workflows'
import { Cause, Effect, Exit, Layer } from 'effect'
import { Extension } from '../../harness/index.ts'
import { Faux, Model, ModelError } from '../../model/index.ts'
import { HarnessWorkflow } from '../index.ts'

/** A `WorkflowStep` that runs each step once, and the steps it ran. */
const steps = () => {
  const ran: Array<WorkflowStepConfig & { readonly name: string }> = []
  const layer = Layer.succeed(WorkflowStep, {
    // `task` provides each step's context to its Effect before handing it over.
    do: <T, E>({ effect, name, retries, timeout }: WorkflowTaskOptions<T, never, never, E>) => {
      ran.push({ name, ...(retries && { retries }), ...(timeout !== undefined && { timeout }) })
      return effect
    },
    sleep: () => Effect.void,
    sleepUntil: () => Effect.void,
    waitForEvent: () => Effect.die('no events in this test'),
  })
  return { ran, layer }
}

describe('HarnessWorkflow', () => {
  test('answers the input in one step named turn', async () => {
    const { ran, layer } = steps()
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const faux = yield* Faux.make()
        yield* faux.setResponses([Faux.message('Hello there.')])
        const run = HarnessWorkflow.make({
          settings: { model: faux.ref() },
          step: { retries: { limit: 2, delay: '1 second' } },
        })
        return yield* run({ content: 'Hi' }).pipe(
          Effect.provide([layer, Model.layer([faux.provider])]),
        )
      }),
    )
    expect(result).toEqual({ status: 'done', answer: 'Hello there.' })
    expect(ran).toEqual([{ name: 'turn', retries: { limit: 2, delay: '1 second' } }])
  })

  test('returns an unanswered run as a result, with its reason', async () => {
    const { layer } = steps()
    const faux = await Effect.runPromise(Faux.make())
    const result = await Effect.runPromise(
      HarnessWorkflow.turn('ask', 'Hi').pipe(Effect.provide([layer, Model.layer([faux.provider])])),
    )
    expect(result).toEqual({ status: 'unanswered', reason: 'no_model', answer: '' })
  })

  test('gives each step a fresh conversation', async () => {
    const { ran, layer } = steps()
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const faux = yield* Faux.make()
        yield* faux.setResponses([Faux.message('One.'), Faux.message('Two.')])
        const options = { settings: { model: faux.ref() } }
        const turn = (name: string, content: string) =>
          HarnessWorkflow.turn(name, content, options).pipe(
            Effect.provide(Model.layer([faux.provider])),
          )
        const first = yield* turn('first', 'Count')
        const second = yield* turn('second', `After ${first.answer}`)
        const requests = yield* faux.requests
        return { second, requests }
      }).pipe(Effect.provide(layer)),
    )
    expect(ran.map(({ name }) => name)).toEqual(['first', 'second'])
    expect(result.second).toEqual({ status: 'done', answer: 'Two.' })
    // The second request carries only its own input: nothing of the first step's conversation.
    const users = result.requests[1]?.messages.filter((message) => message.role === 'user')
    expect(users?.map((message) => message.content)).toEqual(['After One.'])
  })

  test('fails the attempt with TurnFailed when the model fails, for Cloudflare to retry', async () => {
    const { layer } = steps()
    const error = await Effect.runPromise(
      Effect.gen(function* () {
        const faux = yield* Faux.make()
        yield* faux.setResponses([
          new ModelError.ModelError({
            message: 'overloaded',
            reason: new ModelError.Retryable({}),
          }),
        ])
        return yield* HarnessWorkflow.turn('ask', 'Hi', {
          settings: { model: faux.ref(), retry: { enabled: false } },
        }).pipe(Effect.provide([layer, Model.layer([faux.provider])]), Effect.flip)
      }),
    )
    expect(error).toBeInstanceOf(HarnessWorkflow.TurnFailed)
    expect(error).toMatchObject({ reason: 'model_error', detail: 'overloaded' })
  })

  test('dies when the loop cannot open, since no retry would fix it', async () => {
    const { layer } = steps()
    const faux = await Effect.runPromise(Faux.make())
    const bad = Extension.make('bad', {
      sections: [Extension.section('Bad', () => Effect.succeed(''))],
    })
    const exit = await Effect.runPromiseExit(
      HarnessWorkflow.turn('ask', 'Hi', { extensions: [bad] }).pipe(
        Effect.provide([layer, Model.layer([faux.provider])]),
      ),
    )
    expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true)
  })

  test('dies on params that are not an Input', async () => {
    const { ran, layer } = steps()
    const faux = await Effect.runPromise(Faux.make())
    const exit = await Effect.runPromiseExit(
      HarnessWorkflow.make()({ prompt: 'Hi' }).pipe(
        Effect.provide([layer, Model.layer([faux.provider])]),
      ),
    )
    expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true)
    expect(ran).toEqual([])
  })
})
