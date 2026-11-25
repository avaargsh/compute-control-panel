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
    },
    Node: Element,
    fetch: async () => ({
      ok: true,
      json: async () => ({ data_mode: "demo", items, generated_at: "2026-11-25T00:00:00Z" }),
    }),
    setInterval: () => {},
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
