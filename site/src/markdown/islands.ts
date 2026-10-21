/** The islands the guides may use, shared by the markdown plugin and the views. */
import { Schema } from 'effect'

export const islandAttributes = {
  /** A collapsed section; the guides write it as GitHub's `<details>`. */
  Details: Schema.Struct({ summary: Schema.String }),
}
