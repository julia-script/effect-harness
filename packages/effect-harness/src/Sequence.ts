import * as Schema from 'effect/Schema'

export const SequenceSchema = Schema.Natural.pipe(Schema.brand('Sequence'))
export type Sequence = typeof SequenceSchema.Type
export const make = (value: number) => SequenceSchema.make(value)
