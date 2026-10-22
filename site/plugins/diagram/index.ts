/** A Mermaid diagram from the guides, drawn as the site's own SVG at build time. */
import { layoutGraph, layoutSequence } from './layout.ts'
import { parse } from './parse.ts'
import { idOf, renderGraph, renderSequence } from './render.ts'

export const renderDiagram = async (source: string): Promise<string> => {
  const diagram = parse(source)
  const id = idOf(source)
  return diagram._tag === 'Graph'
    ? renderGraph(id, await layoutGraph(diagram))
    : renderSequence(id, layoutSequence(diagram))
}
