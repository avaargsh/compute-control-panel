const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { test } = require("node:test");
const vm = require("node:vm");

const app = fs.readFileSync(path.join(__dirname, "assets/app.js"), "utf8");

class Element {
  constructor() {
    this.children = [];
    this.classList = { add() {}, remove() {}, toggle() {} };
  }
  append(child) { this.children.push(child); }
  replaceChildren() { this.children = []; }
  setAttribute() {}
  removeAttribute() {}
  addEventListener(type, handler) { this[type] = handler; }
}
function text(element) {
  return [element.textContent, ...element.children.flatMap(text)].filter(value => value !== undefined);
}

async function preview(items) {
  const ids = new Map();
  const get = (id) => {
    if (!ids.has(id)) ids.set(id, new Element());
    return ids.get(id);
  };
  const gpus = new Element();
  gpus.dataset = { page: "gpus" };
  // Run the actual embedded JS, including its first fetch and tab listener.
  const ready = vm.runInNewContext(app, {
    document: {
      getElementById: get,
      createElement: () => new Element(),
      querySelectorAll: () => [gpus],
      addEventListener() {},
    },
    Node: Element,
    fetch: async () => ({
      ok: true,
      json: async () => ({ data_mode: "demo", items, generated_at: "2026-11-25T00:00:00Z" }),
    }),
    setTimeout: () => {},
    clearTimeout: () => {},
  }, { filename: "app.js" });
  await ready;
  const dashboard = text(get("view"));
  gpus.click();
  return { dashboard, gpus: text(get("view")) };
}

function quantity(capacity, allocatable = capacity) {
  return {
    source: "synthetic/demo",
    resource: { kind: "NodeGPUResource", name: "demo-node/example.com/gpu" },
    reported_quantity: { resource_name: "example.com/gpu", capacity, allocatable },
  };
}

function metric(page, title) {
  const label = page.indexOf(title);
  assert.notEqual(label, -1, `missing metric: ${title}`);
  return page[label + 1];
}

test("missing GPU reports are distinguishable from explicitly reported zero", async () => {
  const absent = await preview([]);
  assert.equal(metric(absent.dashboard, "GPU 资源上报容量"), "未上报");
  assert.equal(metric(absent.gpus, "Node 上报 Capacity"), "未上报");
  assert.equal(metric(absent.gpus, "Node 上报 Allocatable"), "未上报");
  const zero = await preview([quantity("0")]);
  assert.equal(metric(zero.dashboard, "GPU 资源上报容量"), "0");
  assert.equal(metric(zero.gpus, "Node 上报 Allocatable"), "0");
});

test("an incomplete GPU report never becomes a misleading partial total", async () => {
  const result = await preview([quantity("4", "3"), quantity("bad", "2")]);
  assert.equal(metric(result.dashboard, "GPU 资源上报容量"), "数据不完整");
  assert.equal(metric(result.gpus, "Node 上报 Capacity"), "数据不完整");
  assert.equal(metric(result.gpus, "Node 上报 Allocatable"), "5");
  assert.equal(result.gpus.includes("bad"), false);
});

test("reported GPU quantities are summed without rounding", async () => {
  const result = await preview([quantity("9007199254740993"), quantity("2", "0")]);
  assert.equal(metric(result.dashboard, "GPU 资源上报容量"), "9007199254740995");
  assert.equal(metric(result.gpus, "Node 上报 Allocatable"), "9007199254740993");
});

async function transitions({ observedIn = 0 } = {}) {
  let now = Date.parse("2026-11-25T00:00:00Z");
  let result = "success";
  let reads = 0;
  const timers = new Map();
  let nextTimer = 0;
  const ids = new Map();
  const get = (id) => {
    if (!ids.has(id)) ids.set(id, new Element());
    return ids.get(id);
  };
  const events = new Element();
  const clusters = new Element();
  clusters.dataset = { page: "clusters" };
  class Clock extends Date {
    static now() { return now; }
  }

  await vm.runInNewContext(app, {
    Date: Clock,
    Node: Element,
    document: {
      getElementById: get,
      createElement: () => new Element(),
      querySelectorAll: () => [clusters],
      addEventListener: events.addEventListener.bind(events),
      visibilityState: "visible",
    },
    fetch: async () => {
      reads++;
      if (result === "failure") return { ok: false };
      return {
        ok: true,
        json: async () => ({
          data_mode: "demo",
          generated_at: new Date(now).toISOString(),
          items: [{
            source: "synthetic/demo",
            status: "AVAILABLE",
            observed_at: new Date(now + observedIn).toISOString(),
            expires_at: new Date(now + 1000).toISOString(),
            resource: { kind: "Node", name: "fixture-node" },
          }],
        }),
      };
    },
    setInterval: () => {},
    setTimeout: (fn, ms) => { timers.set(++nextTimer, { fn, time: now + ms }); return nextTimer; },
    clearTimeout: (id) => { timers.delete(id); },
  }, { filename: "app.js" });

  clusters.click();
  return {
    view: () => text(get("view")),
    readCount: () => reads,
    timers: () => timers.size,
    advance: (ms, run = true) => {
      now += ms;
      if (!run) return;
      for (const [id, timer] of [...timers]) {
        if (timer.time <= now) {
          timers.delete(id);
          timer.fn();
        }
      }
    },
    resume: () => events.visibilitychange?.(),
    refresh: async (next) => { result = next; await get("refresh").click(); },
  };
}

test("healthy observation becomes stale at its expiry without an HTTP request", async () => {
  const ui = await transitions();
  assert.equal(metric(ui.view(), "Ready 节点"), "1");
  ui.advance(999);
  assert.equal(metric(ui.view(), "Ready 节点"), "1");
  ui.advance(1);
  assert.equal(metric(ui.view(), "Ready 节点"), "0");
  assert.equal(ui.view().includes("观察过期"), true);
  assert.equal(ui.readCount(), 1);
});

test("a future observation stays unknown until its start time", async () => {
  const ui = await transitions({ observedIn: 500 });
  assert.equal(metric(ui.view(), "Ready 节点"), "0");
  ui.advance(499);
  assert.equal(metric(ui.view(), "Ready 节点"), "0");
  ui.advance(1);
  assert.equal(metric(ui.view(), "Ready 节点"), "1");
  ui.advance(500);
  assert.equal(metric(ui.view(), "Ready 节点"), "0");
  assert.equal(ui.readCount(), 1);
});

test("restoring a tab rechecks expiry even if timers were throttled", async () => {
  const ui = await transitions();
  ui.advance(1001, false);
  ui.resume();
  assert.equal(metric(ui.view(), "Ready 节点"), "0");
  assert.equal(ui.readCount(), 1);
});

test("a failed read clears previous data, and only a fresh success restores it", async () => {
  const ui = await transitions();
  await ui.refresh("failure");
  assert.equal(ui.view().includes("fixture-node"), false);
  assert.equal(ui.timers(), 0);
  await ui.refresh("success");
  assert.equal(metric(ui.view(), "Ready 节点"), "1");
  ui.advance(1000);
  assert.equal(metric(ui.view(), "Ready 节点"), "0");
});
