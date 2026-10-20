/**
 * The stack: a single-node Docker Swarm, a volume for the loops' data, and the agent service.
 *
 *   alchemy deploy examples/docker/alchemy.run.ts    # the local Docker engine, or a remote context
 *   alchemy destroy examples/docker/alchemy.run.ts
 */
import * as Alchemy from 'alchemy'
import * as Docker from 'alchemy/Docker'
import { Effect } from 'effect'
import Agent from './src/Agent.ts'

export default Alchemy.Stack(
  'EffectHarnessDocker',
  { providers: Docker.providers(), state: Alchemy.localState() },
  Effect.gen(function* () {
    const agent = yield* Agent
    return { ports: agent.ports }
  }),
)
