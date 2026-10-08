/** Private Node resource acquisition boundary; native functions retain their overloads. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as fs from 'node:fs'
import * as promises from 'node:fs/promises'

export class NodeResourceAdapter extends Context.Service<
  NodeResourceAdapter,
  {
    readonly open: typeof promises.open
    readonly opendir: typeof promises.opendir
    readonly lstat: typeof promises.lstat
    readonly statfs: typeof promises.statfs
    readonly watch: typeof fs.watch
  }
>()('effect-harness/internal/NodeResourceAdapter') {}
export const native = NodeResourceAdapter.of({
  open: promises.open,
  opendir: promises.opendir,
  lstat: promises.lstat,
  statfs: promises.statfs,
  watch: fs.watch,
})
export const adapter = Effect.map(Effect.serviceOption(NodeResourceAdapter), (service) =>
  Option.getOrElse(service, () => native),
)
