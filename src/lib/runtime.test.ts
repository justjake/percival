import { expect } from "chai";
import init from "percival-wasm";
import { build } from "./runtime";

function checkProgram({
  src,
  deps,
  results,
  input,
  output,
}: {
  src: string;
  deps: string[];
  results: string[];
  input: Record<string, object[]>;
  output: Record<string, object[]>;
}) {
  const result = build(src);
  expect(result.ok).to.be.true;
  if (!result.ok) {
    // unreachable, needed for type inference
    throw null;
  }
  expect(result.deps).to.have.members(deps);
  expect(result.results).to.have.members(results);
  const observed = result.evaluate(input);
  expect(observed).to.have.keys(...Object.keys(output));
  for (const key of Object.keys(output)) {
    expect(observed[key]).to.have.deep.members(output[key]);
  }
}

describe("basic compilation", () => {
  it("can build code", async () => {
    await init();
    expect(build("tc(x: 3).").ok).to.be.true;
    expect(build("tc(x:).").ok).to.be.false;
  });

  it("evaluates a simple program", async () => {
    await init();
    const result = build("tc(x: 3).");
    expect(result.ok).to.be.true;
    if (!result.ok) throw null; // unreachable
    expect(result.evaluate({})).to.be.deep.equal({
      tc: [{ x: 3 }],
    });
  });

  it("evaluates transitive closure from input", async () => {
    await init();
    checkProgram({
      src: `
tc(x, y) :- edge(x, y).
tc(x, y) :- tc(x, y: z), edge(x: z, y).
`,
      deps: ["edge"],
      results: ["tc"],
      input: {
        edge: [
          { x: 2, y: 3 },
          { x: 3, y: 4 },
        ],
      },
      output: {
        tc: [
          { x: 2, y: 3 },
          { x: 2, y: 4 },
          { x: 3, y: 4 },
        ],
      },
    });
  });

  it("evaluates transitive closure inline", async () => {
    await init();
    checkProgram({
      src: `
edge(x: "foo", y: "bar").
edge(x: "bar", y: "baz").
tc(x, y) :- edge(x, y).
tc(x, y) :- tc(x, y: z), edge(x: z, y).
`,
      deps: [],
      results: ["edge", "tc"],
      input: {},
      output: {
        edge: [
          { x: "foo", y: "bar" },
          { x: "bar", y: "baz" },
        ],
        tc: [
          { x: "foo", y: "bar" },
          { x: "foo", y: "baz" },
          { x: "bar", y: "baz" },
        ],
      },
    });
  });
});

describe("embedded backtick expressions", () => {
  it("evaluates backtick expressions", async () => {
    await init();
    checkProgram({
      src: `
name(value: \`first + " " + last\`) :- person(first, last).
`,
      deps: ["person"],
      results: ["name"],
      input: {
        person: [
          {
            first: "eric",
            last: "zhang",
          },
          {
            first: "john",
            last: "doe",
          },
        ],
      },
      output: {
        name: [{ value: "eric zhang" }, { value: "john doe" }],
      },
    });
  });

  it("evaluates fibonacci numbers", async () => {
    await init();
    checkProgram({
      src: `
fib(n: 0, x: 0).
fib(n: 1, x: 1).
fib(n: \`n + 1\`, x: \`x1 + x2\`) :-
    fib(n, x: x1),
    fib(n: \`n - 1\`, x: x2),
    \`n < 10\`.
`,
      deps: [],
      results: ["fib"],
      input: {},
      output: {
        fib: [
          { n: 0, x: 0 },
          { n: 1, x: 1 },
          { n: 2, x: 1 },
          { n: 3, x: 2 },
          { n: 4, x: 3 },
          { n: 5, x: 5 },
          { n: 6, x: 8 },
          { n: 7, x: 13 },
          { n: 8, x: 21 },
          { n: 9, x: 34 },
          { n: 10, x: 55 },
        ],
      },
    });
  });
});
