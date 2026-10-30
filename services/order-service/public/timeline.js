// CommerceFlow order timeline page. Talks only to the Order Service it is
// served by. Server text is inserted with textContent, never as HTML.

const STATUS = {
  Created: { label: "Created", tone: "info", icon: "○" },
  PaymentAuthorized: { label: "Payment authorized", tone: "warning", icon: "◔" },
  InventoryReserved: { label: "Stock reserved", tone: "warning", icon: "◑" },
  DeliveryBooked: { label: "Delivery booked", tone: "good", icon: "✓" },
  InventoryReservationFailed: { label: "Reservation failed", tone: "critical", icon: "✕" },
};

const ENTRY_TONE = { Success: "good", Failure: "critical", Waiting: "warning", Info: "info" };

const state = {
  selectedOrderId: null,
  selectedStatus: null,
  lastRequest: null,
  timelineTimer: null,
};

const $ = (id) => document.getElementById(id);

function el(tag, attributes = {}, ...children) {
  const node = document.createElement(tag);
  for (const [name, value] of Object.entries(attributes)) {
    if (name === "class") node.className = value;
    else if (name.startsWith("on")) node.addEventListener(name.slice(2), value);
    else node.setAttribute(name, value);
  }
  for (const child of children) {
    if (child !== null && child !== undefined) node.append(child instanceof Node ? child : String(child));
  }
  return node;
}

function statusChip(status) {
  const info = STATUS[status] ?? { label: status, tone: "info", icon: "•" };
  return el("span", { class: `chip ${info.tone}` }, `${info.icon} ${info.label}`);
}

function time(iso) {
  const date = new Date(iso);
  return date.toLocaleTimeString(undefined, { hour12: false }) + "." + String(date.getMilliseconds()).padStart(3, "0");
}

function newKey() {
  $("idempotency-key").value = crypto.randomUUID();
  state.lastRequest = null;
  $("retry-button").disabled = true;
}

// ------------------------------------------------------------ submission

async function send(request) {
  const response = await fetch("/orders", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Idempotency-Key": request.key },
    body: JSON.stringify(request.body),
  });
  const body = await response.json().catch(() => ({}));
  return { response, body };
}

async function submit(request) {
  const box = $("response");
  box.hidden = false;
  box.replaceChildren(el("span", { class: "muted" }, "Sending…"));
  $("submit-button").disabled = true;
  $("retry-button").disabled = true;
  try {
    const { response, body } = await send(request);
    const replayed = response.headers.get("Idempotent-Replayed") === "true";
    const meaning = {
      201: "Created: a new order was stored.",
      200: replayed ? "Replayed: same key and request, the original order was returned; nothing new was created." : "OK",
      400: "Rejected: invalid request or missing Idempotency-Key.",
      422: "Rejected: this key was already used for a different order.",
      503: "Not stored right now; retry with the same key.",
    }[response.status] ?? "";
    box.replaceChildren(
      ...[
        el("strong", {}, `HTTP ${response.status} `),
        replayed ? el("span", { class: "chip info" }, "Idempotent-Replayed: true") : null,
        el("div", { class: "muted" }, meaning),
        el("pre", {}, JSON.stringify(body, null, 2)),
      ].filter((node) => node !== null),
    );
    state.lastRequest = request;
    if (body.orderId) {
      await refreshOrders();
      selectOrder(body.orderId);
    }
  } catch (error) {
    box.replaceChildren(el("strong", {}, "Request failed"), el("div", { class: "muted" }, error.message));
  } finally {
    $("submit-button").disabled = false;
    $("retry-button").disabled = state.lastRequest === null;
  }
}

function readForm() {
  const option = $("product-id").selectedOptions[0];
  return {
    key: $("idempotency-key").value,
    body: {
      customerId: $("customer-id").value.trim(),
      items: [{ productId: option.value, quantity: Number($("quantity").value), unitPrice: Number(option.dataset.price) }],
    },
  };
}

// ----------------------------------------------------------------- lists

async function refreshOrders() {
  const response = await fetch("/orders?limit=20");
  if (!response.ok) return;
  const { orders } = await response.json();
  $("orders-empty").hidden = orders.length > 0;
  $("order-list").replaceChildren(
    ...orders.map((order) =>
      el(
        "li",
        {},
        el(
          "button",
          {
            type: "button",
            "aria-current": String(order.orderId === state.selectedOrderId),
            onclick: () => selectOrder(order.orderId),
          },
          el("span", { class: "id" }, order.orderId.slice(0, 8)),
          statusChip(order.status),
          el("span", { class: "meta" }, `${order.customerId} · ${order.totalAmount.toFixed(2)}`),
          el("span", { class: "meta" }, time(order.createdAt)),
        ),
      ),
    ),
  );
}

async function refreshReadiness() {
  try {
    const response = await fetch("/ready");
    const body = await response.json();
    const deps = body.dependencies ?? {};
    $("readiness").replaceChildren(
      el("span", { class: `chip ${response.ok ? "good" : "critical"}` }, response.ok ? "✓ Ready" : "✕ Not ready"),
      ` PostgreSQL ${deps.database ?? "?"} · RabbitMQ ${deps.rabbitMq ?? "?"}`,
    );
  } catch {
    $("readiness").replaceChildren(el("span", { class: "chip critical" }, "✕ Order Service unreachable"));
  }
}

// -------------------------------------------------------------- timeline

function selectOrder(orderId) {
  state.selectedOrderId = orderId;
  for (const button of $("order-list").querySelectorAll("button")) {
    button.setAttribute("aria-current", String(button.querySelector(".id").textContent === orderId.slice(0, 8)));
  }
  loadTimeline();
}

async function loadTimeline() {
  clearTimeout(state.timelineTimer);
  const orderId = state.selectedOrderId;
  if (!orderId) return;
  const response = await fetch(`/orders/${encodeURIComponent(orderId)}`);
  if (!response.ok || orderId !== state.selectedOrderId) return;
  const view = await response.json();
  renderTimeline(view);
  if (view.order.status !== state.selectedStatus) {
    // Keep the list's status chip in step with the timeline.
    state.selectedStatus = view.order.status;
    refreshOrders();
  }
  if (view.outcome === "InProgress" || view.publication?.publishedAt === null) {
    state.timelineTimer = setTimeout(loadTimeline, 1000);
  }
}

function renderTimeline(view) {
  const order = view.order;
  $("timeline-empty").hidden = true;
  $("timeline").hidden = false;
  const tone = { Completed: "good", Failed: "critical", InProgress: "warning" }[view.outcome];
  const summary = $("timeline-summary");
  summary.className = `summary ${tone}`;
  summary.textContent = view.summary;

  const accepted = Date.parse(order.createdAt);
  $("timeline-facts").replaceChildren(
    el("dt", {}, "Order"), el("dd", { class: "mono" }, order.orderId),
    el("dt", {}, "Status"), el("dd", {}, statusChip(order.status)),
    el("dt", {}, "Customer"), el("dd", {}, order.customerId),
    el("dt", {}, "Items"), el("dd", {}, order.items.map((item) => `${item.quantity} × ${item.productId} @ ${item.unitPrice.toFixed(2)}`).join(", ")),
    el("dt", {}, "Total"), el("dd", {}, order.totalAmount.toFixed(2)),
    el("dt", {}, "Correlation id"), el("dd", { class: "mono" }, order.correlationId),
  );

  $("timeline-entries").replaceChildren(
    ...view.entries.map((entry) => {
      const offset = Date.parse(entry.at) - accepted;
      const badges = [
        el("span", { class: "badge" }, entry.service),
        entry.eventType ? el("span", { class: "badge" }, entry.eventType) : null,
        entry.simulated ? el("span", { class: "badge simulated" }, "simulated") : null,
        entry.deliveries > 1 ? el("span", { class: "badge" }, `delivered ${entry.deliveries}× · recorded once`) : null,
        entry.eventId ? el("span", { class: "badge mono" }, `event ${entry.eventId.slice(0, 8)}`) : null,
        entry.producerTime ? el("span", { class: "badge" }, `producer time ${time(entry.producerTime)}`) : null,
      ];
      return el(
        "li",
        { class: ENTRY_TONE[entry.outcome] ?? "info" },
        el(
          "div",
          { class: "entry-head" },
          el("span", { class: "entry-title" }, entry.title),
          el("span", { class: "entry-time" }, `${time(entry.at)} · +${Math.max(0, offset)} ms`),
        ),
        el("p", { class: "entry-body" }, entry.description),
        el("div", { class: "badges" }, ...badges),
      );
    }),
  );
}

// ------------------------------------------------------------------ init

$("order-form").addEventListener("submit", (event) => {
  event.preventDefault();
  if (!$("customer-id").value.trim()) {
    $("customer-id").focus();
    return;
  }
  submit(readForm());
});
$("retry-button").addEventListener("click", () => state.lastRequest && submit(state.lastRequest));
$("new-key-button").addEventListener("click", newKey);

newKey();
refreshReadiness();
refreshOrders();
setInterval(refreshOrders, 3000);
setInterval(refreshReadiness, 10000);
