import { compile } from "percival-wasm";
import type { Program } from "percival-wasm/ast/Program";
import Worker from "./runtime.worker?worker&inline";
import type { RelationSet } from "./types";

interface CancellablePromise<T> extends Promise<T> {
  cancel: () => void;
}

type EvalPromise = CancellablePromise<RelationSet>;

type CompilerResultOk = {
  ok: true;
  evaluate: (deps: RelationSet) => EvalPromise;
  deps: string[];
  results: string[];
  ast: Program | undefined;
};

type CompilerResultErr = {
  ok: false;
  errors: string;
};

export type CompilerResult = CompilerResultOk | CompilerResultErr;

export function build(src: string): CompilerResult {
  let result = compile(src);
  if (result.is_ok()) {
    const code = result.js();
    return {
      ok: true,
      evaluate: (deps) => {
        const worker = new Worker();
        let rejectCb: (reason?: any) => void;
        const promise: Partial<EvalPromise> = new Promise((resolve, reject) => {
          rejectCb = reject;
          worker.addEventListener("message", (event) => {
            resolve(event.data);
            worker.terminate();
          });
          worker.addEventListener("error", (event) => {
            reject(new Error(event.message));
            worker.terminate();
          });
          worker.postMessage({ type: "source", code });
          worker.postMessage({ type: "eval", deps });
        });
        promise.cancel = () => {
          worker.terminate();
          rejectCb(new Error("Promise was cancelled by user"));
        };
        return promise as EvalPromise;
      },
      deps: result.deps()!,
      results: [...result.results()!],
      ast: result.ast(),
    };
  } else {
    return { ok: false, errors: result.err()! };
  }
}
