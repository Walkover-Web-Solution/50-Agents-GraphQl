import { GraphQLScalarType, Kind, ValueNode } from "graphql";

function parseLiteralNode(ast: ValueNode): unknown {
  switch (ast.kind) {
    case Kind.STRING:
    case Kind.BOOLEAN:
      return ast.value;
    case Kind.INT:
    case Kind.FLOAT:
      return Number(ast.value);
    case Kind.OBJECT: {
      const value = Object.create(null) as Record<string, unknown>;
      for (const field of ast.fields ?? []) {
        value[field.name.value] = parseLiteralNode(field.value);
      }
      return value;
    }
    case Kind.LIST:
      return (ast.values ?? []).map((item) => parseLiteralNode(item));
    case Kind.NULL:
      return null;
    default:
      return null;
  }
}

export const jsonScalar = new GraphQLScalarType({
  name: "JSON",
  description: "Arbitrary JSON scalar",
  parseValue(value: unknown): unknown {
    return value;
  },
  serialize(value: unknown): unknown {
    return value;
  },
  parseLiteral(ast): unknown {
    return parseLiteralNode(ast);
  },
});
