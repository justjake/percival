import { expect } from "chai";
import { marshal, unmarshal } from "./marshal";
import type { CellData } from "./notebook";

function check(data: CellData[]) {
  const text = marshal(data);
  const newData = unmarshal(text);
  expect(newData).to.deep.equal(data);
}

describe("unmarshal", () => {
  it("can decode a cell on the first line", () => {
    const text = `╔═╡ Code
edge(x: 1, y: 2).
edge(x: 2, y: 3).
edge(x: 2, y: 4).
`;
    const newData = unmarshal(text);
    expect(newData).to.deep.equal([
      {
        type: "code",
        value: text.split("\n").slice(1).join("\n").trimEnd(),
        hidden: false,
      },
    ]);
  });
});

describe("marshal function", () => {
  it("encodes empty data", () => {
    check([]);
  });

  it("encodes simple notebook", () => {
    check([
      {
        type: "markdown",
        value: "Hello\n\nWorld123 **bold text**\n",
        hidden: false,
      },
      {
        type: "markdown",
        value: "\n\t\n\n",
        hidden: true,
      },
      {
        type: "code",
        value: "tc(x) :- \n\ny(x).",
        hidden: false,
      },
      {
        type: "code",
        value: "\ny(x: 5). // initialize\n",
        hidden: true,
      },
    ]);
  });

  it("preserves line endings", () => {
    check([
      {
        type: "markdown",
        value: "\r\nHello\n\r\nWorld123 \r\n\n**bold text**\r\n\n",
        hidden: false,
      },
    ]);
  });

  it("encodes a plot", () => {
    check([
      {
        type: "plot",
        value: `aapl => Plot.area(aapl, {x1: "Date", y1: 0, y2: "Close"}).plot()`,
        hidden: false,
      },
    ]);
  });
});
