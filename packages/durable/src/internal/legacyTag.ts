import * as Schema from 'effect/Schema'
import * as SchemaTransformation from 'effect/SchemaTransformation'

// The source Struct owns every encoded key. A decoded-only tag cannot enter
// journal frames, receipts or native Activity cache values through this codec.
export const tagged = <const Tag extends string, const Fields extends Schema.Struct.Fields>(
  tag: Tag,
  fields: Fields,
): Schema.decodeTo<Schema.toType<Schema.TaggedStruct<Tag, Fields>>, Schema.Struct<Fields>> => {
  const domain = Schema.TaggedStruct(tag, fields)
  return Schema.Struct(fields).pipe(
    Schema.decodeTo(
      Schema.toType(domain),
      SchemaTransformation.transform({
        // TypeScript cannot express a generic Struct's mapped field construction.
        // Both sides have precisely the same fields; only this private _tag is added/removed.
        decode: (value) => ({ ...value, _tag: tag }) as typeof domain.Type,
        encode: (value) => {
          const rest = { ...value }
          Reflect.deleteProperty(rest, '_tag')
          return rest as Schema.Struct<Fields>['Type']
        },
      }),
    ),
  )
}
