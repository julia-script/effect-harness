import { Lexer, type Token, type Tokens } from 'marked'
import type { Html, HtmlBuilder } from 'foldkit/html'
import { resolveLink } from './Content'

export const slug = (text: string): string =>
  text
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '')
export const headings = (body: string) =>
  Lexer.lex(body).flatMap((token) =>
    token.type === 'heading' && token.depth === 2
      ? [{ title: token.text, id: slug(token.text) }]
      : [],
  )

export const render = <M>(
  body: string,
  source: string,
  h: HtmlBuilder<M>,
  onCopy: (text: string) => M,
): ReadonlyArray<Html | string> => {
  const inline = (tokens: ReadonlyArray<Token> = []): Array<Html | string> =>
    tokens.flatMap((token): Array<Html | string> => {
      switch (token.type) {
        case 'text':
          return token.tokens ? inline(token.tokens) : [token.text]
        case 'escape':
          return [token.text]
        case 'codespan':
          return [h.code([], [token.text])]
        case 'strong':
          return [h.strong([], inline(token.tokens))]
        case 'em':
          return [h.em([], inline(token.tokens))]
        case 'del':
          return [h.del([], inline(token.tokens))]
        case 'br':
          return [h.br([])]
        case 'link':
          return [h.a([h.Href(resolveLink(token.href, source))], inline(token.tokens))]
        default:
          return [token.raw]
      }
    })
  const blocks = (tokens: ReadonlyArray<Token> = []): Array<Html | string> =>
    tokens.flatMap((token): Array<Html | string> => {
      switch (token.type) {
        case 'space':
          return []
        case 'heading': {
          const attrs = [h.Id(slug(token.text))]
          if (token.depth === 1) return []
          if (token.depth === 2) return [h.h2(attrs, inline(token.tokens))]
          return [h.h3(attrs, inline(token.tokens))]
        }
        case 'paragraph':
          return [h.p([], inline(token.tokens))]
        case 'text':
          return [h.p([], token.tokens ? inline(token.tokens) : [token.text])]
        case 'code':
          return [
            h.div(
              [h.Class('code-block')],
              [
                h.div(
                  [h.Class('code-toolbar')],
                  [
                    h.span([], [token.lang || 'text']),
                    h.button(
                      [h.OnClick(onCopy(token.text)), h.Type('button'), h.AriaLabel('Copy code')],
                      ['Copy'],
                    ),
                  ],
                ),
                h.pre([], [h.code([], [token.text])]),
              ],
            ),
          ]
        case 'list': {
          const items = token.items.map((item: Tokens.ListItem) => h.li([], blocks(item.tokens)))
          return [token.ordered ? h.ol([], items) : h.ul([], items)]
        }
        case 'blockquote':
          return [h.blockquote([], blocks(token.tokens))]
        case 'hr':
          return [h.hr([])]
        case 'table':
          return [
            h.div(
              [h.Class('table-scroll')],
              [
                h.table(
                  [],
                  [
                    h.thead(
                      [],
                      [
                        h.tr(
                          [],
                          token.header.map((cell: Tokens.TableCell) =>
                            h.th([], inline(cell.tokens)),
                          ),
                        ),
                      ],
                    ),
                    h.tbody(
                      [],
                      token.rows.map((row: Tokens.TableCell[]) =>
                        h.tr(
                          [],
                          row.map((cell: Tokens.TableCell) => h.td([], inline(cell.tokens))),
                        ),
                      ),
                    ),
                  ],
                ),
              ],
            ),
          ]
        default:
          return [token.raw]
      }
    })
  return blocks(Lexer.lex(body))
}
