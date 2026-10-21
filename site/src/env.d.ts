/** A guide's code blocks in document order, highlighted; `null` for a diagram. */
declare module '*.md?code' {
  const blocks: ReadonlyArray<string | null>
  export default blocks
}

/** A source file, raw and highlighted. */
declare module '*?highlight' {
  const source: { readonly raw: string; readonly html: string }
  export default source
}
