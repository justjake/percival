import type { Aggregate } from "@/../crates/percival-wasm/pkg/ast/Aggregate";
import type { Clause } from "@/../crates/percival-wasm/pkg/ast/Clause";
import type { Fact } from "@/../crates/percival-wasm/pkg/ast/Fact";
import type { Import } from "@/../crates/percival-wasm/pkg/ast/Import";
import type { Literal } from "@/../crates/percival-wasm/pkg/ast/Literal";
import type { Rule } from "@/../crates/percival-wasm/pkg/ast/Rule";
import type { Program } from "percival-wasm/ast/Program";
import type { Value } from "percival-wasm/ast/Value";
import { format as formatSql } from "sql-formatter";
import initSqlJs from "sql.js";
import sqlJsWasm from "sql.js/dist/sql-wasm.wasm?url";
import { Relation, type RelationSet } from "./types";

const SQLite = await initSqlJs({
  locateFile: () => sqlJsWasm,
});

const DefaultDB = new SQLite.Database();

export function debugExec(compiler: SqlCompiler | undefined): RelationSet {
  if (compiler === undefined) {
    return {
      [`sqlite.warning`]: Relation([{ message: "No SQL" }]),
    };
  }

  const program = compiler.compile();
  const sqlString = program.toString();

  try {
    const execResult = DefaultDB.exec(sqlString);
    const result: RelationSet = {};
    for (
      let statementIndex = 0;
      statementIndex < execResult.length;
      statementIndex++
    ) {
      const statementResult = execResult[statementIndex];
      const indexToColumn = statementResult.columns;
      const objs = new Array(statementResult.values.length);
      result[`sqlite.${program.outputs[statementIndex]}`] = objs;
      for (
        let rowIndex = 0;
        rowIndex < statementResult.values.length;
        rowIndex++
      ) {
        const row = statementResult.values[rowIndex];
        const obj: Record<string, unknown> = {};
        objs[rowIndex] = obj;
        for (let colIndex = 0; colIndex < row.length; colIndex++) {
          obj[indexToColumn[colIndex]] = row[colIndex];
        }
      }
    }
    return result;
  } catch (error: any) {
    return {
      [`sqlite.error`]: Relation([
        {
          message: error.message,
        },
      ]),
    };
  }
}

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

function must<T>(message: string, val: T | undefined): T {
  if (val === undefined) {
    throw new Error(`must not be undefined: ${message}`);
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

function statementList(...statements: Sql[]): Sql<"list"> {
  return {
    type: "list",
    separator: ";\n",
    terms: statements,
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

export function sqlToStringPretty(sql: Sql | string): string {
  return formatSql(typeof sql === "string" ? sql : sqlToString(sql), {
    indent: "  ",
    // trying to pick a language that treats `||` as a single infix operator
    // the default separates these into ` | | ` which is bad
    language: "plsql",
  });
}

export function sqlToString(sql: Sql): string {
  switch (sql.type) {
    case "op":
      return `${sqlToString(sql.lhs)} ${sql.operator} ${sqlToString(sql.rhs)}`;
    case "raw":
      return sql.raw;
    case "with":
      return `WITH RECURSIVE ${sql.bindings.map(sqlToString).join(",\n")}`;
    case "binding":
      return `${sql.name}(${sql.columnNames.join(", ")}) AS (${sqlToString(
        sql.as,
      )})`;
    case "list":
      return sql.terms.map(sqlToString).join(sql.separator ?? "\n");
    case "values":
      return `VALUES ${sql.rows.map(sqlToString).join(", ")}`;
    case "tuple":
      return `(${sql.values.map(sqlToString).join(", ")})`;
    case "select":
      const columns = sql.resultColumns
        .map(({ expr, as }) => `${sqlToString(expr)} AS ${as}`)
        .join(", ");
      const parts = [
        `SELECT ${columns}`,
        sql.from?.terms?.length && `FROM ${sqlToString(sql.from)}`,
        sql.where?.terms?.length && `WHERE ${sqlToString(sql.where)}`,
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
    return raw(`(${jsToSql(value.Expr)})`);
  }

  if ("Aggregate" in value) {
    // TODO: aggregate
    return raw(`AGGREGATE(${JSON.stringify(value.Aggregate)})`);
    // unreachable(value.Aggregate);
  }

  unreachable(value);
}

const dottedPath = /([a-zA-Z]\w*\.)+([a-zA-Z]\w*)/g;

// Super janky, just to make things easier to test.
function jsToSql(js: string): string {
  let result = js;
  result = result.replaceAll("+", "||");
  result = result.replaceAll(
    dottedPath,
    (match) => match.split(".").at(-1) || match,
  );
  return result;
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
  preOrder: Partial<NodeVisitors<void>>,
  postOrder?: Partial<NodeVisitors<void>>,
): void {
  const visitNode = (
    self: PercivalNode,
    children?: Array<PercivalNode | PercivalNode[]>,
  ) => {
    matchNode(self, preOrder);
    if (children) {
      children.forEach(recurseChildren);
    }
    if (postOrder) {
      matchNode(self, postOrder);
    }
  };
  const recurseChildren = (array: PercivalNode | PercivalNode[]): void => {
    Array.isArray(array)
      ? array.map((node) => matchNode(node, recurse))
      : matchNode(array, recurse);
  };

  const recurse: Required<NodeVisitors<void>> = {
    Aggregate(node) {
      visitNode(node, [node.Aggregate.value, node.Aggregate.subquery]);
    },
    Fact(node) {
      visitNode(node, Object.values(node.Fact.props));
    },
    Import: visitNode,
    Program: function (node: Program): void {
      visitNode(node, [node.imports, node.rules]);
    },
    Rule: function (node: Rule): void {
      visitNode(node, [node.goal, node.clauses]);
    },
    Expr: visitNode,
    Binding: function (node: { Binding: [string, Value] }): void {
      visitNode(node, [node.Binding[1]]);
    },
    Number: visitNode,
    String: visitNode,
    Boolean: visitNode,
    Id: visitNode,
    Literal: visitNode,
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
    return must(
      `have named table ${uniqueName}`,
      this.uniqueNameToTable.get(uniqueName),
    );
  }
}

type TableColumn = `${string}.${string}`;
class Scope<K> {
  constructor(public parent: Scope<unknown> | undefined) {}
  bindings = new Map<string, Value | TableColumn>();
  relatedColumns = new DAG<string, TableColumn>();
  children = new Map<K, Scope<K>>();
  uniqueTableNames = new Namer<Fact>();

  bind(id: string, binding: Value | TableColumn) {
    if (this.has(id)) {
      throw new Error(`Duplicate binding for ${id}`);
    }

    this.bindings.set(id, binding);
    // TODO: is this correct? Or should we manage related columns with separate
    // scoping?
    if (typeof binding === "string") {
      this.relatedColumns.add(id, binding);
    }
  }

  resolve(id: string, term: "column" | "body" = "body"): Sql {
    const scope = this.scope(id);
    if (!scope) {
      // throw new Error(`No binding for ${id} in any scope`);
      return raw(`TODO('Not bound in any scope: ${id}')`);
    }
    const binding = scope.bindings.get(id);
    if (!binding) {
      throw new Error(`Missing binding for ${id}`);
    }
    if (typeof binding === "string") {
      return term === "column" ? raw(binding) : raw(id);
    }

    return must(
      `resolve bound id ${id}`,
      matchNode(binding, {
        Id: (node) => {
          return this.resolve(node.Id, term);
        },
        Literal: valueToSql,
        Expr: valueToSql,
        Aggregate(node) {
          return raw(
            `TODO('Resolve id bound to aggregate: ${JSON.stringify(node)}')`,
          );
        },
      }),
    );
  }

  relate(id: string, column: TableColumn) {
    const scope = this.scope(id);
    // TODO: is this correct? Or should we manage related columns with separate
    // scoping?
    if (scope) {
      scope.relatedColumns.add(id, column);
    } else {
      this.bind(id, column);
    }
  }

  scope(id: string): Scope<unknown> | undefined {
    if (this.bindings.has(id)) {
      return this;
    }

    return this.parent?.scope(id);
  }

  has(id: string): boolean {
    return Boolean(this.bindings.has(id) || this.parent?.has(id));
  }

  childScope(key: K): Scope<K> {
    const child = this.children.get(key) || new Scope(this);
    this.children.set(key, child);
    return child;
  }

  enter<R>(key: K, fn: (scope: Scope<K>) => R): R {
    return fn(this.childScope(key));
  }
}

class SqlCompiler {
  constructor(public readonly program: Program) {}

  sql(): string {
    const comp = this.compile();
    return comp.toString();
  }

  compile() {
    const goals = new Map<string, Sql<"binding">>();
    const rootScope = new Scope<Rule>(undefined);

    {
      let currentScope = rootScope;

      // Scan the program for ID bindings to columns or expressions.
      visit(
        this.program,
        {
          Rule: (rule) => {
            currentScope = currentScope.childScope(rule);
            for (const clause of rule.clauses) {
              matchNode(clause, {
                Fact: (fact) => {
                  const { name, props } = fact.Fact;
                  const uniqueName =
                    currentScope.uniqueTableNames.uniqueTableName(
                      fact.Fact,
                      name,
                    );

                  for (const [column, value] of Object.entries(props)) {
                    const id = matchNode(value, {
                      Id: (id) => id.Id,
                    });
                    if (id) {
                      currentScope.relate(id, `${uniqueName}.${column}`);
                    }
                  }
                },
                Binding: (binding) => {
                  const [id, value] = binding.Binding;
                  if (
                    matchNode(value, {
                      Id: (node) => node.Id === id,
                    })
                  ) {
                    throw new Error(`Binding to itself: ${id}`);
                  }
                  currentScope.bind(id, value);
                },
              });
            }
          },
        },
        {
          Rule: () => {
            currentScope = must(
              `restore scope`,
              currentScope.parent,
            ) as Scope<Rule>;
          },
        },
      );
    }

    for (const rule of this.program.rules) {
      rootScope.enter(rule, (currentScope) => {
        const term: Sql<"binding"> = goals.get(rule.goal.name) || {
          type: "binding",
          name: rule.goal.name,
          columnNames: Object.keys(rule.goal.props),
          as: {
            type: "list",
            separator: "\nUNION\n",
            terms: [],
          },
        };
        goals.set(rule.goal.name, term);

        const select: Sql<"select"> = {
          type: "select",
          resultColumns: [],
        };
        term.as.terms.push(select);

        // Set up projections for each result column
        const projected = new Set<string>();
        for (const [column, value] of Object.entries(rule.goal.props)) {
          select.resultColumns.push({
            expr: must(
              `project result column ${column}`,
              matchNode(value, {
                Id(node) {
                  projected.add(node.Id);
                  return currentScope.resolve(node.Id, "column");
                },
                Literal: valueToSql,
                Expr: valueToSql,
                Aggregate: valueToSql,
              }),
            ),
            as: column,
          });
        }

        const tables = new Set(
          Array.from(currentScope.relatedColumns.edges.values())
            .flatMap((edges) => Array.from(edges))
            .map((col) => col.split(".")[0]),
        );
        select.from = commaSeparated(
          ...Array.from(tables).map((uniqueName) => {
            const originalName =
              currentScope.uniqueTableNames.originalTableName(uniqueName);
            if (originalName === uniqueName) {
              return raw(originalName);
            }
            return raw(`${originalName} AS ${uniqueName}`);
          }),
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
              const uniqueName = currentScope.uniqueTableNames.uniqueTableName(
                Fact,
                name,
              );
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
      });
    }

    type CompileResult = {
      statements: Sql<"list">;
      outputs: string[];
      toString(): string;
      lastGoalString(): string;
    };

    const result: CompileResult = {
      statements: statementList(),
      outputs: Array.from(goals.keys()),
      toString: () => sqlToStringPretty(result.statements),
      lastGoalString: () =>
        sqlToStringPretty(result.statements.terms.at(-1) ?? ""),
    };

    for (const [goalName, goal] of goals.entries()) {
      // We need to emit one statement per goal...
      // lots of duplication.
      // todo: temporary table?
      result.statements.terms.push({
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

    return result;
  }
}

export function compileToSql(program: Program) {
  const compiler = new SqlCompiler(program);
  return compiler;
}
