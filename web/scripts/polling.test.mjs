import ts from "typescript";

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import vm from "node:vm";

const web = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
function moduleFor(name, globals = {}) {
  const output = ts.transpileModule(fs.readFileSync(path.join(web, "src/lib", name + ".ts"), "utf8"), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  }).outputText;
  const exports = {};
  vm.runInNewContext(output, { exports, ...globals });
  return exports;
}
function setup(hidden = false) {
  let sequence = 0;
  const timers = new Map();
  const listeners = new Set();
  const document = {
    visibilityState: hidden ? "hidden" : "visible",
    addEventListener: (_, callback) => listeners.add(callback),
    removeEventListener: (_, callback) => listeners.delete(callback),
  };
  const { setVisibleInterval } = moduleFor("polling", {
    document,
    setTimeout: (callback) => {
      const id = ++sequence;
      timers.set(id, callback);
      return id;
    },
    clearTimeout: (id) => timers.delete(id),
  });
  return {
    setVisibleInterval,
    timers,
    listeners,
    visibility(hidden) {
      document.visibilityState = hidden ? "hidden" : "visible";
      for (const callback of listeners) callback();
    },
    tick() {
      const entries = [...timers];
      timers.clear();
      for (const [, callback] of entries) callback();
    },
  };
}
async function settle() {
  for (let i = 0; i < 8; i++) await Promise.resolve();
}
test("hidden tabs make no scheduled requests; returning refreshes immediately and cleanup stops work", async () => {
  const s = setup(true);
  let calls = 0;
  const stop = s.setVisibleInterval(() => {
    calls++;
  }, 5000);
  s.tick();
  await settle();
  assert.equal(calls, 0);
  assert.equal(s.timers.size, 0);
  s.visibility(false);
  await settle();
  assert.equal(calls, 1);
  assert.equal(s.timers.size, 1);
  s.visibility(true);
  assert.equal(s.timers.size, 0);
  s.visibility(false);
  await settle();
  assert.equal(calls, 2);
  stop();
  s.tick();
  s.visibility(false);
  await settle();
  assert.equal(calls, 2);
  assert.equal(s.listeners.size, 0);
  assert.equal(s.timers.size, 0);
});
test("a slow poll cannot overlap on timer or visibility changes; dispose while running cannot revive it", async () => {
  const s = setup();
  let calls = 0;
  let release;
  const stop = s.setVisibleInterval(() => {
    calls++;
    return new Promise((resolve) => {
      release = resolve;
    });
  }, 2000);
  s.tick();
  assert.equal(calls, 1);
  s.visibility(true);
  s.visibility(false);
  s.tick();
  assert.equal(calls, 1);
  release();
  await settle();
  assert.equal(s.timers.size, 1);
  s.tick();
  assert.equal(calls, 2);
  stop();
  release();
  await settle();
  assert.equal(s.timers.size, 0);
});
test("a failed background refresh retries on the next interval", async () => {
  const s = setup();
  let calls = 0;
  const stop = s.setVisibleInterval(async () => {
    if (++calls === 1) throw Error("temporary");
  }, 1000);
  s.tick();
  await settle();
  assert.equal(s.timers.size, 1);
  s.tick();
  await settle();
  assert.equal(calls, 2);
  stop();
});

// Exercise the component's actual loader and polling effect with a virtual
// clock. Testing only the helper misses callbacks that discard the Promise.
function approvalPollingSource() {
  const source = fs.readFileSync(path.join(web, "src/components/approval-records.tsx"), "utf8");
  const ast = ts.createSourceFile("approval-records.tsx", source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  const component = ast.statements.find(
    (node) => ts.isFunctionDeclaration(node) && node.name?.text === "ApprovalRecords",
  );
  assert.ok(component);
  let load;
  let effect;
  function visit(node) {
    if (ts.isVariableDeclaration(node) && node.name.getText(ast) === "load" && ts.isCallExpression(node.initializer)) {
      load = node.initializer.arguments[0].getText(ast);
    }
    if (
      ts.isCallExpression(node) &&
      node.expression.getText(ast) === "React.useEffect" &&
      node.getText(ast).includes("setVisibleInterval")
    ) {
      effect = node.arguments[0].getText(ast);
    }
    ts.forEachChild(node, visit);
  }
  visit(component);
  assert.ok(load && effect);
  return ts.transpileModule(`globalThis.load = (${load}); globalThis.start = (${effect});`, {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  }).outputText;
}

async function slowApprovalPolling(reject) {
  let now = 0;
  let nextID = 0;
  const timers = new Map();
  const state = { loading: true, refreshing: false, rows: [], total: 0, pendingRows: [], error: "", pendingError: "" };
  const setTimeout = (callback, delay = 0) => {
    const id = ++nextID;
    timers.set(id, { at: now + delay, callback });
    return id;
  };
  let completed = 0;
  const delayed = (value) =>
    new Promise((resolve, fail) => {
      setTimeout(() => {
        completed++;
        if (reject) fail(new Error("slow approval API unavailable"));
        else resolve(value);
      }, 6000);
    });
  const setters = Object.fromEntries(
    Object.keys(state).map((key) => [
      `set${key[0].toUpperCase()}${key.slice(1)}`,
      (value) => {
        state[key] = value;
      },
    ]),
  );
  const { setVisibleInterval } = moduleFor("polling", {
    setTimeout,
    clearTimeout: (id) => timers.delete(id),
  });
  const context = vm.createContext({
    ...setters,
    setVisibleInterval,
    request: { current: 0 },
    taskId: undefined,
    page: 1,
    pageSize: 20,
    filter: {},
    api: {
      interceptHistoryPage: () => delayed({ items: [{ id: 101 }], total: 1 }),
      interceptPending: () => delayed([{ id: 101 }]),
    },
  });
  vm.runInContext(approvalPollingSource(), context);
  const stop = context.start();
  for (;;) {
    const next = [...timers].sort((a, b) => a[1].at - b[1].at || a[0] - b[0])[0];
    if (!next || next[1].at > 60000) break;
    now = next[1].at;
    timers.delete(next[0]);
    next[1].callback();
    // Drain both VM and host Promise chains, including allSettled and finally.
    for (let i = 0; i < 30; i++) await Promise.resolve();
  }
  stop();
  const stoppedState = JSON.stringify(state);
  // Requests already sent may finish after unmount, but cannot publish results
  // or restart the poller.
  for (const [id, timer] of [...timers]) {
    timers.delete(id);
    timer.callback();
  }
  for (let i = 0; i < 30; i++) await Promise.resolve();
  assert.equal(timers.size, 0, "cleanup stops future polling");
  assert.equal(JSON.stringify(state), stoppedState, "cleanup invalidates in-flight responses");
  assert.ok(completed >= 4, "multiple slow loads completed");
  return state;
}

test("approval polling publishes slow successful responses and clears the loading state", async () => {
  const state = await slowApprovalPolling(false);
  assert.equal(state.loading, false);
  assert.equal(state.refreshing, false);
  assert.equal(state.rows[0]?.id, 101);
  assert.equal(state.total, 1);
  assert.equal(state.pendingRows[0]?.id, 101);
  assert.equal(state.error, "");
  assert.equal(state.pendingError, "");
});

test("approval polling publishes slow failures so refresh becomes available again", async () => {
  const state = await slowApprovalPolling(true);
  assert.equal(state.loading, false);
  assert.equal(state.refreshing, false);
  assert.equal(state.error, "slow approval API unavailable");
  assert.equal(state.pendingError, "slow approval API unavailable");
});
