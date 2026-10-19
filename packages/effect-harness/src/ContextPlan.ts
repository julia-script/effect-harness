/**
 * Read-only positional context preparation. The caller owns the registry lease and serialized
 * persistence of protocol/fact values. These APIs do not activate the conversation runtime.
 */
export {
  ContextPlanError,
  DesiredSchema,
  DraftSchema,
  PlanSchema,
  PositioningSchema,
  FrameSchema,
  ToolPositionSchema,
  TranslationSchema,
  render,
  plan,
  translate,
} from './internal/ContextPlan.js'
export type {
  Desired,
  Draft,
  Plan,
  Positioning,
  Frame,
  ToolPosition,
  Translation,
  Renderer,
  RenderOptions,
} from './internal/ContextPlan.js'
export {
  ProtocolSchema,
  LegacyModeSchema,
  activate as activateProtocol,
  inherit as inheritProtocol,
  options as protocolOptions,
  project as projectWithProtocol,
  read as readWithProtocol,
} from './internal/ContextProtocol.js'
export type { Protocol, LegacyMode } from './internal/ContextProtocol.js'
