"use strict";

// Standalone, dependency-free product-preview UI. The production React
// console will reuse the same observation wire contract, not this demo server.
const $ = (id) => document.getElementById(id);
const state = { page: "dashboard", items: [], generatedAt: "", loaded: false, failed: false };
const pages = {
  dashboard: ["Dashboard", "资源总览", "从可信来源和观察时间理解集群与 GPU 资源。"],
  clusters: ["Clusters", "集群与节点", "查看节点 Ready、UID、状态原因和观察时间。"],
  gpus: ["GPUs", "GPU 资源上报", "仅展示节点上报的 capacity 与 allocatable；不计算空闲 GPU。"],
};
const statusNames = {
  AVAILABLE: "正常",
  DEGRADED: "需关注",
  UNAVAILABLE: "不可用",
  UNKNOWN: "未验证",
  STALE: "观察过期",
};
const statusValues = new Set(Object.keys(statusNames));

function node(tag, className, text) {
  const element = document.createElement(tag);
  if (className) element.className = className;
  if (text !== undefined && text !== null) element.textContent = String(text);
  return element;
}

function clear(element) {
  element.replaceChildren();
}

function timeLabel(value) {
  if (!value || Number.isNaN(Date.parse(value))) return "无有效时间";
  return new Date(value).toLocaleString("zh-CN", { hour12: false });
}

function effectiveStatus(item) {
  if (!item || !statusValues.has(item.status)) return "UNKNOWN";
  if (!item.observed_at || !item.expires_at ||
      Number.isNaN(Date.parse(item.observed_at)) ||
      Number.isNaN(Date.parse(item.expires_at))) return "UNKNOWN";
  if (Date.now() >= Date.parse(item.expires_at)) return "STALE";
  if (Date.now() < Date.parse(item.observed_at)) return "UNKNOWN";
  return item.status;
}

function pill(item) {
  const status = effectiveStatus(item);
  return node("span", "pill " + status, statusNames[status]);
}

function metric(title, value, hint) {
  const card = node("article", "metric-card");
  card.append(node("div", "metric-label", title));
  card.append(node("div", "metric-value", value));
  card.append(node("div", "metric-hint", hint));
  return card;
}

function panel(title, subtitle, labels, rows) {
  const box = node("section", "panel");
  const header = node("div", "panel-head");
  header.append(node("h2", "", title));
  header.append(node("span", "", subtitle));
  box.append(header);
  const wrap = node("div", "table-wrap");
  const table = node("table");
  const thead = node("thead");
  const tr = node("tr");
  for (const label of labels) tr.append(node("th", "", label));
  thead.append(tr);
  table.append(thead);
  const tbody = node("tbody");
  for (const row of rows) {
    const line = node("tr");
    for (const cell of row) {
      const td = node("td");
      if (typeof cell === "string" || typeof cell === "number") {
        td.textContent = String(cell);
      } else if (cell instanceof Node) {
        td.append(cell);
      }
      line.append(td);
    }
    tbody.append(line);
  }
  table.append(tbody);
  wrap.append(table);
  box.append(wrap);
  if (rows.length === 0) box.append(node("div", "empty-state", "没有可显示的观察记录"));
  return box;
}

function note(text) {
  return node("p", "note", text);
}

function clusterItems() {
  return state.items.filter((item) => item.resource?.kind === "Node");
}

function gpuItems() {
  return state.items.filter((item) => item.resource?.kind === "NodeGPUResource");
}

function number(value) {
  try {
    if (!/^[0-9]+$/.test(String(value))) return 0n;
    return BigInt(String(value));
  } catch {
    return 0n;
  }
}

function sumReported(items, key) {
  return items.reduce((sum, item) => sum + number(item.reported_quantity?.[key]), 0n).toString();
}

function nodeRows(items) {
  return items.map((item) => [
    node("strong", "name", item.resource?.name ?? "未知节点"),
    node("span", "mono", item.resource?.cluster_id ?? "—"),
    pill(item),
    node("span", "mono", item.reason_code || "—"),
    timeLabel(item.observed_at),
  ]);
}

function renderDashboard(root) {
  const nodes = clusterItems();
  const gpus = gpuItems();
  const cards = node("div", "card-grid");
  cards.append(metric("纳管集群（演示）", new Set(nodes.map((item) => item.resource?.cluster_id)).size,
    "集群连接未实际建立"));
  cards.append(metric("节点上报数量", nodes.length, "包含正常与异常节点"));
  cards.append(metric("GPU 资源上报容量", sumReported(gpus, "capacity"),
    "来自 Node.status.capacity · 非空闲数量"));
  root.append(cards);
  root.append(panel("节点状态", "观察来源：synthetic/demo", ["节点", "集群", "状态", "诊断代码", "观察时间"], nodeRows(nodes)));
  root.append(note("注意：节点的 allocatable 是 Kubernetes 声明的可分配资源总量，不代表当前空闲 GPU；GPU 物理 UID、占用和释放状态需要独立证据。"));
}

function renderClusters(root) {
  const nodes = clusterItems();
  const cards = node("div", "card-grid");
  cards.append(metric("节点总量", nodes.length, "模拟的 Node 对象"));
  cards.append(metric("Ready 节点", nodes.filter((item) => effectiveStatus(item) === "AVAILABLE").length,
    "仅表示节点 Ready 观察"));
  cards.append(metric("需关注 / 不可用", nodes.filter((item) => !["AVAILABLE"].includes(effectiveStatus(item))).length,
    "包含 DEGRADED、UNKNOWN 与 STALE"));
  root.append(cards);
  root.append(panel("集群节点", "只读 · 不支持创建或修改", ["节点", "集群", "状态", "诊断原因", "观察时间"], nodeRows(nodes)));
  root.append(note("源数据缺失、过期或状态不明确时，界面会展示 UNKNOWN/STALE；不将未知状态默认渲染为正常。"));
}

function renderGPUs(root) {
  const items = gpuItems();
  const cards = node("div", "card-grid");
  cards.append(metric("GPU 扩展资源类型", items.length, "按节点与资源名称统计"));
  cards.append(metric("Node 上报 Capacity", sumReported(items, "capacity"), "节点声明总量 · 非可用卡数"));
  cards.append(metric("Node 上报 Allocatable", sumReported(items, "allocatable"), "非空闲数 · 不扣除运行中 Pod"));
  root.append(cards);
  const rows = items.map((item) => [
    node("strong", "name", item.reported_quantity?.resource_name ?? "未知资源类型"),
    node("span", "mono", item.resource?.name?.split("/")[0] ?? "—"),
    node("span", "mono", item.reported_quantity?.capacity ?? "—"),
    node("span", "mono", item.reported_quantity?.allocatable ?? "—"),
    pill(item),
    node("span", "mono", item.reason_code || "—"),
    timeLabel(item.observed_at),
  ]);
  root.append(panel("GPU 资源记录", "不是设备清单，也不是实际使用率", ["资源名", "节点", "容量", "Allocatable", "状态", "证据原因", "观察时间"], rows));
  root.append(note("该视图不包含 PCIe 拓扑、HBM、GPU 使用率、物理设备 UID 或 GPU RELEASED。引入真实 DRA/HAMi 证据前，不得用于成本 FINAL。"));
}

function render() {
  const page = pages[state.page];
  $("page-crumb").textContent = page[0];
  $("page-title").textContent = page[1];
  $("page-desc").textContent = page[2];
  document.querySelectorAll("[data-page]").forEach((button) => {
    const isActive = button.dataset.page === state.page;
    button.classList.toggle("active", isActive);
    if (isActive) button.setAttribute("aria-current", "page");
    else button.removeAttribute("aria-current");
  });
  const root = $("view");
  clear(root);
  if (!state.loaded || state.failed) {
    root.append(node("div", "empty-state", state.failed ?
      "无法读取本地 Demo 数据。请检查本地演示服务后重试。" : "正在加载观察数据…"));
    return;
  }
  if (state.page === "dashboard") renderDashboard(root);
  else if (state.page === "clusters") renderClusters(root);
  else renderGPUs(root);
}

async function refresh() {
  const button = $("refresh");
  button.disabled = true;
  $("connection").textContent = "读取中…";
  $("connection").classList.remove("error");
  try {
    const response = await fetch("/api/v1/observations", { cache: "no-store", headers: { Accept: "application/json" } });
    if (!response.ok) throw new Error("read failed");
    const payload = await response.json();
    if (payload.data_mode !== "demo" || !Array.isArray(payload.items) ||
        !payload.items.every((item) => item.source === "synthetic/demo")) {
      throw new Error("unsafe source/mode");
    }
    state.items = payload.items;
    state.generatedAt = payload.generated_at;
    state.loaded = true;
    state.failed = false;
    $("connection").textContent = "本地模拟数据已就绪";
    $("source").textContent = "synthetic/demo";
    $("observed").textContent = timeLabel(payload.generated_at);
  } catch {
    // Do not retain a green view after an error or silently reuse stale data.
    state.items = [];
    state.loaded = true;
    state.failed = true;
    $("connection").textContent = "Demo 数据无法读取";
    $("connection").classList.add("error");
    $("source").textContent = "未验证";
    $("observed").textContent = "—";
  } finally {
    button.disabled = false;
    render();
  }
}

document.querySelectorAll("[data-page]").forEach((button) => {
  button.addEventListener("click", () => {
    const target = button.dataset.page;
    if (!Object.hasOwn(pages, target)) return;
    state.page = target;
    render();
  });
});
$("refresh").addEventListener("click", refresh);
setInterval(() => { if (state.loaded && !state.failed) render(); }, 15000);
refresh();
