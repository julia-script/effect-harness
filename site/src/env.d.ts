/** A guide's code blocks in document order: highlighted code, or a diagram as SVG. */
declare module '*.md?code' {
  const blocks: ReadonlyArray<string>
  export default blocks
}

/** A source file, raw and highlighted. */
declare module '*?highlight' {
  const source: { readonly raw: string; readonly html: string }
  export default source
}

/** A Mermaid diagram, drawn as SVG. */
declare module '*.mmd?diagram' {
  const svg: string
  export default svg
}
