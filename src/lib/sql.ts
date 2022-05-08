import type { Aggregate } from "@/../crates/percival-wasm/pkg/ast/Aggregate";
import type { Clause } from "@/../crates/percival-wasm/pkg/ast/Clause";
import type { Fact } from "@/../crates/percival-wasm/pkg/ast/Fact";
import type { Import } from "@/../crates/percival-wasm/pkg/ast/Import";
import type { Literal } from "@/../crates/percival-wasm/pkg/ast/Literal";
import type { Rule } from "@/../crates/percival-wasm/pkg/ast/Rule";
import type { Program } from "percival-wasm/ast/Program";
import type { Value } from "percival-wasm/ast/Value";

interface SqlTypes {
  raw: { raw: string };

  with: {
    bindings: Sql<"binding">[];
  };

  binding: {
    name: string;
    columnNames: string[];
    as: Sql<"list">;
  };

  list: {
    separator?: string;
    terms: Sql[];
  };

  values: {
    rows: Sql<"tuple">[];
  };

  tuple: {
    values: Sql[];
  };

  select: {
    resultColumns: Array<{
      expr: Sql;
      as: string;
    }>;
    from?: Sql<"list">;
    where?: Sql<"list">;
  };

  op: {
    operator: string;
    lhs: Sql;
    rhs: Sql;
  };
}

type Sql<T extends keyof SqlTypes = keyof SqlTypes> = {
  [Type in keyof SqlTypes]: SqlTypes[Type] & { type: Type };
}[T];

function must<T>(val: T | undefined): T {
  if (val === undefined) {
    throw new Error("must not be undefined");
  }
  return val;
}

const NOTHING: Sql<"raw"> = {
  type: "raw",
  raw: "",
};

function unreachable(val: never): never {
  throw new Error("unreachable");
}

function op(lhs: Sql, operator: string, rhs: Sql): Sql<"op"> {
  return {
    type: "op",
    operator,
    lhs,
    rhs,
  };
}

function tuple(...values: Sql[]): Sql<"tuple"> {
  return {
    type: "tuple",
    values,
  };
}

function raw(literal: string): Sql<"raw"> {
  return {
    type: "raw",
    raw: literal,
  };
}

function list(...terms: Sql[]): Sql<"list"> {
  return {
    type: "list",
    terms,
  };
}

function commaSeparated(...terms: Sql[]): Sql<"list"> {
  return {
    type: "list",
    separator: ", ",
    terms,
  };
}

function toString(sql: Sql): string {
  switch (sql.type) {
    case "op":
      return `${toString(sql.lhs)} ${sql.operator} ${toString(sql.rhs)}`;
    case "raw":
      return sql.raw;
    case "with":
      return `WITH ${sql.bindings.map(toString).join(",\n")}`;
    case "binding":
      return `${sql.name}(${sql.columnNames.join(", ")}) AS (${toString(
        sql.as,
      )})`;
    case "list":
      return sql.terms.map(toString).join(sql.separator ?? "\n");
    case "values":
      return `VALUES ${sql.rows.map(toString).join(", ")}`;
    case "tuple":
      return `(${sql.values.map(toString).join(", ")})`;
    case "select":
      const columns = sql.resultColumns
        .map(({ expr, as }) => `${toString(expr)} AS ${as}`)
        .join(", ");
      const parts = [
        `SELECT ${columns}`,
        sql.from?.terms?.length && `FROM ${toString(sql.from)}`,
        sql.where?.terms?.length && `WHERE ${toString(sql.where)}`,
      ]
        .filter(Boolean)
        .join("\n");
      return parts;
    default:
      unreachable(sql);
  }
}

function valueToSql(value: Value): Sql {
  if ("Id" in value) {
    return raw(value.Id);
  }

  if ("Literal" in value) {
    if ("Number" in value.Literal) {
      return raw(String(value.Literal.Number));
    }

    if ("String" in value.Literal) {
      // TODO: quoting
      const quoted = value.Literal.String.replace(/'/g, "''");
      return raw(`'${quoted}'`);
    }

    if ("Boolean" in value.Literal) {
      return value.Literal.Boolean ? raw("1") : raw("0");
    }

    unreachable(value.Literal);
  }

  if ("Expr" in value) {
    return raw(`(${value.Expr})`);
  }

  if ("Aggregate" in value) {
    // TODO: aggregate
    return raw(`AGGREGATE(${JSON.stringify(value.Aggregate)})`);
    // unreachable(value.Aggregate);
  }

  unreachable(value);
}

class DAG<From, To> {
  edges = new Map<From, Set<To>>();

  add(from: From, to: To): void {
    const set = this.edges.get(from) ?? new Set();
    set.add(to);
    this.edges.set(from, set);
  }
}

type PercivalNode =
  | Aggregate
  | Clause
  | Fact
  | Import
  | Literal
  | Program
  | Rule
  | Value;

type Binding = { Binding: [string, Value] };

interface RegularNodes {
  Aggregate: { Aggregate: Aggregate };
  Fact: { Fact: Fact };
  Import: Import;
  Program: Program;
  Rule: Rule;
}

interface WackyNodes {
  Clause: Clause;
  Literal: Literal;
  Value: Value;
}

type NodeVisitors<T = void> = {
  [Type in keyof RegularNodes]: (node: RegularNodes[Type]) => T;
} & Visitors<Clause, T> &
  Visitors<Literal, T> &
  Visitors<Value, T>;

type NodeNameMap = {
  [K in keyof NodeVisitors<void>]: NodeVisitors<void>[K] extends (
    arg: infer V,
  ) => void | undefined
    ? V
    : never;
};

type Matchable = NodeNameMap[keyof NodeNameMap];

type NodeVisitorsFor<Node extends Matchable, T> = {
  [K in keyof NodeVisitors<T> as NodeNameMap[K] extends Node
    ? K
    : never]: NodeVisitors<T>[K];
};

type nvs = NodeVisitorsFor<Literal, void>;
type a = NodeVisitorsFor<Matchable, void>;

type Visitors<N, R> = {
  [T in N as keyof T]: (node: T) => R;
};

function visit(
  root: PercivalNode,
  visitors: Partial<NodeVisitors<void>>,
): void {
  const visitSelf = (self: PercivalNode) => matchNode(self, visitors);
  const recurseChildren = (array: PercivalNode | PercivalNode[]): void => {
    Array.isArray(array)
      ? array.map((node) => matchNode(node, recurse))
      : matchNode(array, recurse);
  };

  const recurse: Required<NodeVisitors<void>> = {
    Aggregate(node) {
      visitSelf(node);
      recurseChildren(node.Aggregate.value);
      recurseChildren(node.Aggregate.subquery);
    },
    Fact(node) {
      visitSelf(node);
      recurseChildren(Object.values(node.Fact.props));
    },
    Import: visitSelf,
    Program: function (node: Program): void {
      visitSelf(node);
      recurseChildren(node.imports);
      recurseChildren(node.rules);
    },
    Rule: function (node: Rule): void {
      visitSelf(node);
      recurseChildren(node.goal);
      recurseChildren(node.clauses);
    },
    Expr: visitSelf,
    Binding: function (node: { Binding: [string, Value] }): void {
      visitSelf(node);
      recurseChildren(node.Binding[1]);
    },
    Number: visitSelf,
    String: visitSelf,
    Boolean: visitSelf,
    Id: visitSelf,
    Literal: visitSelf,
  };

  return matchNode(root, recurse);
}

function matchNode<R>(
  node: PercivalNode,
  visitors: Partial<NodeVisitors<R>>,
): R | undefined {
  if ("operator" in node) {
    return visitors.Aggregate?.({ Aggregate: node });
  }

  if ("Aggregate" in node) {
    return visitors.Aggregate?.(node);
  }

  if ("Literal" in node) {
    if (visitors.Literal) {
      return visitors.Literal(node);
    }
    return matchNode(node.Literal, visitors);
  }

  if ("Fact" in node) {
    return visitors.Fact?.(node);
  }

  if ("Expr" in node) {
    return visitors.Expr?.(node);
  }

  if ("Binding" in node) {
    return visitors.Binding?.(node);
  }

  if ("name" in node && "props" in node) {
    return visitors.Fact?.({ Fact: node });
  }

  if ("name" in node && "uri" in node) {
    return visitors.Import?.(node);
  }

  if ("Number" in node) {
    return visitors.Number?.(node);
  }

  if ("String" in node) {
    return visitors.String?.(node);
  }

  if ("Boolean" in node) {
    return visitors.Boolean?.(node);
  }

  if ("Id" in node) {
    return visitors.Id?.(node);
  }

  if ("goal" in node) {
    return visitors.Rule?.(node);
  }

  if ("rules" in node) {
    return visitors.Program?.(node);
  }

  unreachable(node);
}

class Namer<K> {
  uniqueNameCount = new Map<string, number>();
  uniqueNameToTable = new Map<string, string>();
  keyToUniqueName = new Map<K, string>();

  uniqueTableName(key: K, table: string): string {
    const memo = this.keyToUniqueName.get(key);
    if (memo) {
      return memo;
    }

    const count = this.uniqueNameCount.get(table) ?? 0;
    const nextCount = count + 1;
    const uniqueName = count === 0 ? table : `${table}${nextCount}`;
    this.uniqueNameToTable.set(uniqueName, table);
    this.uniqueNameCount.set(table, nextCount);

    this.keyToUniqueName.set(key, uniqueName);
    return uniqueName;
  }

  originalTableName(uniqueName: string): string {
    return must(this.uniqueNameToTable.get(uniqueName));
  }
}

type Bound = `expr:${string}` | `column:${string}`;

class SqlCompiler {
  constructor(public readonly program: Program) {}

  sql(): string {
    const goals = new Map<string, Sql<"binding">>();
    const idToColumn = new DAG<string, string>();
    const idToBinding = new Map<string, Value>();
    const isSourceColumn = new Set<string>();
    const uniqueTableNames = new Namer<Fact>();

    // TODO: scopes.
    const resolveId = (id: string): Sql => {
      const binding = idToBinding.get(id);
      if (binding) {
        console.log("resolve binding", binding);
        return must(
          matchNode(binding, {
            Id(node) {
              return resolveId(node.Id);
            },
            Literal: valueToSql,
            Expr: valueToSql,
            Aggregate(node) {
              return raw(`TODO(Aggregate(${JSON.stringify(node.Aggregate)}))`);
            },
          }),
        );
      }
      const columns = must(Array.from(idToColumn.edges.get(id) ?? []));
      const notSource = columns.find((column) => !isSourceColumn.has(column));
      if (notSource === undefined) {
        return raw(`TODO(resolveId(${id}))`);
      }
      return raw(must(notSource));
    };

    visit(this.program, {
      Rule: (rule) => {
        for (const clause of rule.clauses) {
          matchNode(clause, {
            Fact: (fact) => {
              const { name, props } = fact.Fact;
              const uniqueName = uniqueTableNames.uniqueTableName(
                fact.Fact,
                name,
              );

              for (const [column, value] of Object.entries(props)) {
                const id = matchNode(value, {
                  Id: (id) => id.Id,
                });
                if (id) {
                  idToColumn.add(id, `${uniqueName}.${column}`);
                }
              }
            },
            Binding: (binding) => {
              const [id, value] = binding.Binding;
              if (idToBinding.has(id)) {
                throw new Error(`Duplicate binding: ${id}`);
              }
              if (
                matchNode(value, {
                  Id: (node) => node.Id === id,
                })
              ) {
                throw new Error(`Binding to itself: ${id}`);
              }
              idToBinding.set(id, value);
            },
          });
        }
      },
    });

    console.log(idToColumn.edges);

    for (const rule of this.program.rules) {
      const term: Sql<"binding"> = goals.get(rule.goal.name) || {
        type: "binding",
        name: rule.goal.name,
        columnNames: Object.keys(rule.goal.props),
        as: list(),
      };
      goals.set(rule.goal.name, term);

      if (rule.clauses.length === 0) {
        if (term.as.terms.length > 0) {
          term.as.terms.push(raw("UNION"));
        }

        term.as.terms.push({
          type: "values",
          rows: [tuple(...Object.values(rule.goal.props).map(valueToSql))],
        });
        continue;
      }

      const select: Sql<"select"> = {
        type: "select",
        resultColumns: [],
      };
      term.as.terms.push(select);

      for (const [column, value] of Object.entries(rule.goal.props)) {
        console.log(column, value);
        select.resultColumns.push({
          expr: must(
            matchNode(value, {
              Id(node) {
                const sourceTableColumn = resolveId(node.Id);
                // XXX hacky
                isSourceColumn.add(toString(sourceTableColumn));
                return sourceTableColumn;
              },
              Literal: valueToSql,
              Expr: valueToSql,
            }),
          ),
          as: column,
        });
      }

      const tables = new Set(
        Array.from(idToColumn.edges.values())
          .flatMap((edges) => Array.from(edges))
          .map((col) => col.split(".")[0]),
      );
      select.from = commaSeparated(
        ...Array.from(tables).map((uniqueName) =>
          raw(
            `${uniqueTableNames.originalTableName(
              uniqueName,
            )} AS ${uniqueName}`,
          ),
        ),
      );

      // Constraints... the hard part
      const where: Sql<"list"> = {
        type: "list",
        separator: " AND ",
        terms: [],
      };
      select.where = where;

      const joins = new DAG</* id */ string, /* column */ string>();
      rule.clauses.forEach((clause) =>
        matchNode(clause, {
          Fact({ Fact }) {
            const { name, props } = Fact;
            const uniqueName = uniqueTableNames.uniqueTableName(Fact, name);
            for (const [column, value] of Object.entries(props)) {
              const addEqConstraint = (node: Value) => {
                where.terms.push(
                  op(raw(`${uniqueName}.${column}`), "=", valueToSql(node)),
                );
              };
              matchNode(value, {
                Literal: addEqConstraint,
                Expr: addEqConstraint,
                Id(node) {
                  joins.add(node.Id, `${uniqueName}.${column}`);
                  const [left, right] = Array.from(
                    joins.edges.get(node.Id) ?? [],
                  ).slice(-2);
                  if (left && right) {
                    // Join!
                    where.terms.push(op(raw(left), "=", raw(right)));
                  }
                },
              });
            }
          },
          Expr(expr) {
            where.terms.push(valueToSql(expr));
          },
          Binding({ Binding }) {
            // Handled elsewhere.
          },
        }),
      );
    }

    // SQL can only emit a single thingy...
    const goalNames = Array.from(goals.keys());
    const goalName = goalNames[goalNames.length - 1];
    const goal = goals.get(goalName);

    if (!goal) {
      return "(no goals)";
    }

    return toString({
      type: "list",
      terms: [
        {
          type: "with",
          bindings: Array.from(goals.values()),
        },
        raw(`SELECT * FROM ${goalName}`),
      ],
    });
  }
}

export function compileToSql(program: Program) {
  const compiler = new SqlCompiler(program);
  return compiler;
}
