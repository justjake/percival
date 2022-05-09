import type { Aggregate } from "@/../crates/percival-wasm/pkg/ast/Aggregate";
import type { Clause } from "@/../crates/percival-wasm/pkg/ast/Clause";
import type { Fact } from "@/../crates/percival-wasm/pkg/ast/Fact";
import type { Import } from "@/../crates/percival-wasm/pkg/ast/Import";
import type { Literal } from "@/../crates/percival-wasm/pkg/ast/Literal";
import type { Rule } from "@/../crates/percival-wasm/pkg/ast/Rule";
import type { Program } from "percival-wasm/ast/Program";
import type { Value } from "percival-wasm/ast/Value";
import { format as formatSql } from "sql-formatter";
import initSqlJs, {
  type Database,
  type QueryExecResult,
  type Statement,
  type SqlJsStatic,
} from "sql.js";
import sqlJsWasm from "sql.js/dist/sql-wasm.wasm?url";
import { Relation, type RelationSet, type Row } from "./types";
import { javascript } from "@codemirror/lang-javascript";

let SQLite: SqlJsStatic;

async function withTemporaryDB<T>(
  fn: (db: Database) => Promise<T>,
): Promise<T> {
  SQLite ??= await initSqlJs({
    locateFile: () => {
      const origin = import.meta.env.VITE_VERCEL_URL
        ? `https://${import.meta.env.VITE_VERCEL_URL}`
        : undefined;
      try {
        return new URL(sqlJsWasm, origin).href;
      } catch (error) {
        return sqlJsWasm;
      }
    },
  });
  const db = new SQLite.Database();
  try {
    return await fn(db);
  } finally {
    db.close();
  }
}

export function debugExec(
  compiler: SqlCompiler | undefined,
  inputs: RelationSet,
  load: (url: string) => Promise<Relation>,
): Promise<RelationSet> {
  if (compiler === undefined) {
    return Promise.resolve({
      [`sqlite.warning`]: Relation([{ message: "No SQL" }]),
    });
  }

  if (inputs === undefined) {
    throw new Error(`inputs must be defined`);
  }

  return withTemporaryDB(async (db) => {
    function run(sql: string, arg?: any) {
      // console.log("SQL", sql, arg);
      return db.run(sql, arg);
    }

    function insertData(tableName: string, data: Relation) {
      const example = data[0];
      const columnNames = Object.keys(example);
      const closers: Statement[] = [];
      try {
        run("BEGIN");
        run(
          `CREATE TABLE ${tableName} (${columnNames
            .map((name) => `${name} BLOB`)
            .join(", ")})`,
        );
        const statement = db.prepare(
          `INSERT INTO ${tableName} VALUES (${columnNames
            .map(() => "?")
            .join(",")})`,
        );
        closers.push(statement);
        for (const row of data) {
          statement.run(columnNames.map((x) => row[x] as any));
        }
      } catch (error) {
        run("ROLLBACK");
        throw error;
      } finally {
        closers.forEach((x) => x.free());
        run("COMMIT");
      }
    }

    function readRelation(statementResult: QueryExecResult): Relation {
      const indexToColumn = statementResult.columns;
      const objs = Relation(new Array(statementResult.values.length));
      for (
        let rowIndex = 0;
        rowIndex < statementResult.values.length;
        rowIndex++
      ) {
        const row = statementResult.values[rowIndex];
        const obj: Record<string, unknown> = {};
        objs[rowIndex] = obj as Row;
        for (let colIndex = 0; colIndex < row.length; colIndex++) {
          obj[indexToColumn[colIndex]] = row[colIndex];
        }
      }
      return objs;
    }

    const result: RelationSet = {};

    // Insert all dependencies
    console.log("Inserting dependencies", inputs);
    for (const [name, data] of Object.entries(inputs)) {
      insertData(name, data);
    }

    // Fetch and insert all imports
    for (const import_ of compiler.program.imports) {
      const data = await load(getImportUrl(import_.uri));
      insertData(import_.name, data);
      // get it back out
      const [rows] = db.exec(`SELECT * FROM ${import_.name}`);
      result[`sqlite.${import_.name}`] = readRelation(rows);
    }

    const program = compiler.compile();
    const sqlString = program.toString();

    try {
      for (const exprFunction of compiler.compiledExpressions.values()) {
        db.create_function(exprFunction.sqlName, exprFunction.fn as any);
      }

      const execResult = db.exec(sqlString);
      for (
        let statementIndex = 0;
        statementIndex < execResult.length;
        statementIndex++
      ) {
        const statementResult = execResult[statementIndex];
        result[`sqlite.${program.outputs[statementIndex]}`] =
          readRelation(statementResult);
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
  });
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
      as?: string;
    }>;
    from?: Sql<"list">;
    where?: Sql<"list">;
  };

  op: {
    operator: string;
    lhs: Sql;
    rhs: Sql;
  };

  tableAlias: {
    expr: Sql;
    as: string;
  };

  call: {
    callee: Sql;
    args: Sql[];
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

function todo(message: string, fallback: Sql): Sql<"raw"> {
  return raw(`/* TODO: ${message} */${sqlToString(fallback)}`);
}

export function sqlToStringPretty(sql: Sql | string): string {
  return formatSql(typeof sql === "string" ? sql : sqlToString(sql), {
    indent: "  ",
    // trying to pick a language that treats `||` as a single infix operator
    // the default separates these into ` | | ` which is bad
    language: "plsql",
  });
}

export function sqlToString(sql: Sql | string): string {
  if (typeof sql === "string") {
    return sql;
  }

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
    case "call":
      return `${sqlToString(sql.callee)}(${sql.args
        .map(sqlToString)
        .join(", ")})`;
    case "list":
      return sql.terms.map(sqlToString).join(sql.separator ?? "\n");
    case "values":
      return `VALUES ${sql.rows.map(sqlToString).join(", ")}`;
    case "tuple":
      return `(${sql.values.map(sqlToString).join(", ")})`;
    case "tableAlias":
      return `(${sqlToString(sql.expr)}) AS ${sql.as}`;
    case "select":
      const columns = sql.resultColumns
        .map(({ expr, as: asAlias }) =>
          asAlias ? `${sqlToString(expr)} AS ${asAlias}` : sqlToString(expr),
        )
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

function parseJs(js: string) {
  // Use CodeMirror's Javascript parser to iterate over the expression.
  // TODO: can we share this object?
  const { language } = javascript();
  const parser = language.parser;
  const ast = parser.parse(js);
  return ast;
}

// Janky SQL to JS conversion.
function jsToSql(js: string, scope: Scope<unknown>): string {
  const ast = parseJs(js);

  const replacements: Array<{ from: number; to: number; value: string }> = [];
  let ignore: { type: string; from: number; to: number } | undefined;
  type SyntaxNode = typeof ast.topNode;

  // Used for replacing `+` with `||` concatenation.
  function getStringConcatOperator(node: SyntaxNode) {
    if (node.type.name !== "BinaryExpression") {
      return false;
    }

    const op = node.getChild("ArithOp");
    if (!op) {
      return false;
    }

    const opText = js.slice(op.from, op.to);
    if (opText !== "+") {
      return false;
    }

    const string = node.getChild("String");
    const left = node.firstChild;
    const right = node.lastChild;
    if (
      string ||
      (left && getStringConcatOperator(left)) ||
      (right && getStringConcatOperator(right))
    ) {
      return op;
    }

    return false;
  }

  // TODO: as an alternative to mangling the AST, we could instead create a custom function
  // that takes all variables as arguments, and replace the entire expression with such
  // a function call.
  // https://sql.js.org/documentation/Database.html#%255B%2522create_function%2522%255D
  ast.iterate({
    enter(type, from, to, get) {
      type NodeType = typeof type;
      const replace = (
        value: string,
        node?: { type: NodeType; from: number; to: number },
      ) => {
        const target = node ?? { type, from, to };
        replacements.push({
          from: target.from,
          to: target.to,
          value,
        });
        if (!node) {
          ignore = {
            type: target.type?.name,
            from: target.from,
            to: target.to,
          };
        }
      };

      if (ignore) {
        return;
      }

      const text = js.slice(from, to);

      // Replace id variables with column names,
      // and inline other bindings / expressions
      if (type.name === "VariableName" && scope.has(text)) {
        const resolved = scope.resolve(text, "column");
        replace(sqlToString(resolved));
        return;
      }

      // Math.sqrt(x) -> sqrt(x)
      // https://www.sqlite.org/lang_mathfunc.html
      if (type.name === "MemberExpression" && text.startsWith("Math.")) {
        const [, funcName] = text.split(".");
        replace(funcName);
        return;
      }

      // "foo" + "bar" -> "foo" || "bar"
      // Replace obvious Javascript string math w/ SQLite's concat operator
      if (type.name === "BinaryExpression") {
        const operator = getStringConcatOperator(get());
        if (operator) {
          replace(`||`, operator);
        }
      }
    },
    leave(type, from, to) {
      if (
        ignore &&
        ignore.type === type.name &&
        ignore.from === from &&
        ignore.to === to
      ) {
        ignore = undefined;
      }
    },
  });

  let result = js;
  replacements.sort((a, b) => b.from - a.from);
  for (const { from, to, value } of replacements) {
    result = result.slice(0, from) + value + result.slice(to);
  }
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
  constructor(
    public parent: Scope<unknown> | undefined,
    public compiler: SqlCompiler,
  ) {}
  bindings = new Map<string, Value | TableColumn>();
  relatedColumns = new DAG<string, TableColumn>();
  children = new Map<K, Scope<K>>();
  uniqueTableNames = new Namer<Fact>();

  valueToSql = (value: Value): Sql => this.compiler.compileValue(value, this);

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
      console.warn(`No binding for ${id} in any scope:`, id);
      return todo(`Not bound in any scope: ${id}`, raw(`unbound_id_${id}`));
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
        Literal: this.valueToSql,
        Expr: this.valueToSql,
        Aggregate: (node) => {
          return todo(`Id bound to aggregate: ${id}`, this.valueToSql(node));
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

  all(): Set<string> {
    return new Set([...this.bindings.keys(), ...(this.parent?.all() ?? [])]);
  }

  childScope(key: K): Scope<K> {
    const child = this.children.get(key) || new Scope(this, this.compiler);
    this.children.set(key, child);
    return child;
  }

  enter<R>(key: K, fn: (scope: Scope<K>) => R): R {
    return fn(this.childScope(key));
  }
}

class SqlCompiler {
  constructor(public readonly program: Program) {}

  public compiledExpressions = new Map<
    string,
    {
      fn: Function;
      sqlName: string;
      sql: Sql;
    }
  >();

  sql(): string {
    const comp = this.compile();
    return comp.toString();
  }

  compileValue(value: Value, scope: Scope<unknown>): Sql {
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
        return todo(`Correct quote escaping`, raw(`'${quoted}'`));
      }

      if ("Boolean" in value.Literal) {
        return value.Literal.Boolean ? raw("1") : raw("0");
      }

      unreachable(value.Literal);
    }

    if ("Expr" in value) {
      return this.compileExpression(value.Expr, scope);
    }

    if ("Aggregate" in value) {
      const aggregateFunctionMap: Record<string, string> = {
        mean: "avg",
      };
      // TODO: aggregate
      // return todo(`AGGREGATE ${JSON.stringify(value.Aggregate)}`, raw(`0`));
      const operator =
        aggregateFunctionMap[value.Aggregate.operator] ??
        value.Aggregate.operator;
      const list: Sql<"list"> = {
        type: "list",
        separator: " ",
        terms: [
          {
            type: "call",
            callee: raw(operator),
            args: [this.compileValue(value.Aggregate.value, scope)],
          },
        ],
      };
      return todo("Aggregates are a WIP", list);
    }

    unreachable(value);
  }

  compileExpression(expr: string, scope: Scope<unknown>): Sql {
    if (this.compiledExpressions.has(expr)) {
      const compiled = must("have expr", this.compiledExpressions.get(expr));
      return compiled.sql;
    }

    const sqlFunctionName = `expr${this.compiledExpressions.size}_${expr
      .replaceAll(/[^\w]/g, "")
      .slice(0, 15)}`;
    const ids = new Set<string>();
    const ast = parseJs(expr);
    ast.iterate({
      enter: (type, from, to) => {
        const text = expr.slice(from, to);
        if (type.name === "VariableName" && scope.has(text) && !ids.has(text)) {
          ids.add(text);
        }
      },
    });
    const jsParams = Array.from(ids);
    const sqlParams = jsParams.map((id) => scope.resolve(id, "column"));
    const expressionFunction = new Function(...jsParams, `return ${expr}`);
    const wrapperFunction = (...args: unknown[]) => {
      const result = expressionFunction(...args);
      if (result === null) {
        return result;
      }
      if (typeof result === "object") {
        if (result instanceof Date) {
          return result.toISOString();
        }
        return String(result);
      }
      return result;
    };
    Object.defineProperty(wrapperFunction, "length", {
      value: jsParams.length,
    });
    const sql: Sql<"call"> = {
      type: "call",
      callee: raw(sqlFunctionName),
      args: sqlParams,
    };
    this.compiledExpressions.set(expr, {
      fn: wrapperFunction,
      sql,
      sqlName: sqlFunctionName,
    });
    return sql;
  }

  compile() {
    const goals = new Map<string, Sql<"binding">>();
    const rootScope = new Scope<Rule>(undefined, this);

    {
      let currentScope = rootScope;

      // Scan the program for ID bindings to columns or expressions,
      // and record those bindings into the scope.
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
          Expr: (expr) => {
            // Compile the expression to JS.
            const source = expr.Expr;
            if (this.compiledExpressions.has(source)) {
              return;
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
                Literal: currentScope.valueToSql,
                Expr: currentScope.valueToSql,
                Aggregate: currentScope.valueToSql,
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
                    op(
                      raw(`${uniqueName}.${column}`),
                      "=",
                      currentScope.valueToSql(node),
                    ),
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
              where.terms.push(currentScope.valueToSql(expr));
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
          raw(`SELECT DISTINCT * FROM ${goalName}`),
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

function getImportUrl(url: string) {
  const index = url.indexOf("://");
  if (index === -1) {
    throw new Error(`Unknown URL protocol: ${url}`);
  }
  const protocol = url.slice(0, index + 3);
  const address = url.slice(index + 3);
  switch (protocol) {
    case "http://":
    case "https://":
      return url;
    case "gh://":
      return `https://cdn.jsdelivr.net/gh/${address}`;
    case "npm://":
      return `https://cdn.jsdelivr.net/npm/${address}`;
    default:
      throw new Error(`Unknown URL protocol: ${url}`);
  }
}
