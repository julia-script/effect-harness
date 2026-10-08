export const message = (result: {
  readonly content?:
    | ReadonlyArray<{ readonly type: string; readonly text?: string | undefined }>
    | undefined
}): string =>
  result.content?.flatMap((part) => (part.type === 'text' ? [part.text ?? ''] : [])).join('') ?? ''
