import { useCallback, useEffect, useRef, useState } from "react";
import { api, type ClientInfo, type Notification, type Order, type Snapshot, type Stats, type Subscription } from "./api";

type Feed = Notification & { seq: number };

export default function App() {
  const [snap, setSnap] = useState<Snapshot | null>(null);
  const [feed, setFeed] = useState<Feed[]>([]);
  const [stats, setStats] = useState<Stats | null>(null);
  const [clients, setClients] = useState<ClientInfo[]>([]);
  const [subs, setSubs] = useState<Subscription[]>([]);
  const [connected, setConnected] = useState(false);
  const [busy, setBusy] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const seq = useRef(0);

  // Live stream from the dashboard: a snapshot first, then one message per webhook.
  useEffect(() => {
    const es = new EventSource(api.eventsUrl);
    es.addEventListener("snapshot", (e) => {
      const s = JSON.parse((e as MessageEvent).data) as Snapshot;
      setSnap(s);
      setFeed(s.recent.map((n) => ({ ...n, seq: seq.current++ })).reverse());
      setConnected(true);
    });
    es.addEventListener("notification", (e) => {
      const n = JSON.parse((e as MessageEvent).data) as Notification;
      setFeed((f) => [{ ...n, seq: seq.current++ }, ...f].slice(0, 200));
      setSnap((s) => (s ? applyToSnapshot(s, n) : s));
    });
    es.addEventListener("error", () => setConnected(false));
    es.onopen = () => setConnected(true);
    return () => es.close();
  }, []);

  // Pub/sub admin figures: polled, they live in another service.
  const refreshAdmin = useCallback(async () => {
    try {
      const [st, cl, su] = await Promise.all([api.stats(), api.clients(), api.subscriptions()]);
      setStats(st);
      setClients(cl);
      setSubs(su);
    } catch (err) {
      setMessage(`admin refresh failed: ${(err as Error).message}`);
    }
  }, []);
  useEffect(() => {
    void refreshAdmin();
    const t = setInterval(() => void refreshAdmin(), 3000);
    return () => clearInterval(t);
  }, [refreshAdmin]);

  const run = async (label: string, fn: () => Promise<unknown>) => {
    setBusy(label);
    setMessage(null);
    try {
      await fn();
      setMessage(`${label}: ok`);
    } catch (err) {
      setMessage(`${label}: ${(err as Error).message}`);
    } finally {
      setBusy(null);
      void refreshAdmin();
    }
  };

  const orders = Object.values(snap?.open_orders ?? {}).sort((a, b) => a.order_id.localeCompare(b.order_id));
  const me = clients.find((c) => c.id === snap?.subscriber);

  return (
    <div className="page">
      <header>
        <h1>Order desk</h1>
        <span className={`pill ${connected ? "ok" : "bad"}`}>{connected ? "live" : "disconnected"}</span>
        <span className="muted">
          subscriber <code>{snap?.subscriber_name ?? "…"}</code> · webhook <code>{snap?.webhook_url ?? "…"}</code>
        </span>
      </header>

      <section className="stats">
        <Stat label="events received" value={stats?.received} />
        <Stat label="deliveries created" value={stats?.matched} />
        <Stat label="delivered" value={stats?.delivered} />
        <Stat label="failed" value={stats?.failed} tone={stats && stats.failed > 0 ? "bad" : undefined} />
        <Stat label="my pending" value={me?.pending} tone={me && me.pending > 0 ? "warn" : undefined} />
        <Stat label="my failed" value={me?.failed} tone={me && me.failed > 0 ? "bad" : undefined} />
        <Stat label="subscribers" value={stats?.subscribers} />
        <Stat label="subscriptions" value={stats?.subscriptions} />
      </section>

      <Actions busy={busy} message={message} run={run} orders={orders} />

      <main className="grid">
        <section className="card">
          <h2>Open orders <span className="muted">({orders.length}, kept current by webhooks)</span></h2>
          <OrdersTable orders={orders} selected={selected} onSelect={setSelected} busy={busy} run={run} />
        </section>

        <section className="card feed">
          <h2>Live notifications <span className="muted">(newest first)</span></h2>
          <ul>
            {feed.map((n) => (
              <li key={n.seq} className={`evt ${n.kind}`}>
                <span className="time">{n.at.slice(11, 23)}</span>
                <span className={`tag ${n.kind}`}>{n.kind}</span>
                <span className="who">
                  {n.type}/{n.key} <span className="muted">v{n.version}</span>
                </span>
                {n.changed && n.changed.length > 0 && <span className="changed">changed: {n.changed.join(", ")}</span>}
                {n.type === "Order" && <span className="muted">→ {String(n.data.status)}</span>}
              </li>
            ))}
            {feed.length === 0 && <li className="muted">No notifications yet. Seed some orders.</li>}
          </ul>
        </section>

        <section className="card">
          <h2>My subscriptions</h2>
          <table>
            <thead>
              <tr><th>type</th><th>criteria</th></tr>
            </thead>
            <tbody>
              {subs.map((s) => (
                <tr key={s.id}>
                  <td>{s.type_name}</td>
                  <td><code>{s.keys ? `keys ${s.by}: ${s.keys.join(", ")}` : JSON.stringify(s.filter)}</code></td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>

        <section className="card">
          <h2>Pub/sub clients</h2>
          <table>
            <thead>
              <tr><th>name</th><th>subs</th><th>pending</th><th>failed</th><th>last ok</th></tr>
            </thead>
            <tbody>
              {clients.map((c) => (
                <tr key={c.id} className={c.id === snap?.subscriber ? "me" : ""}>
                  <td>{c.name}</td>
                  <td>{c.subscriptions}</td>
                  <td className={c.pending ? "warn" : ""}>{c.pending}</td>
                  <td className={c.failed ? "bad" : ""}>{c.failed}</td>
                  <td className="muted">{c.last_delivery_ok_at ? c.last_delivery_ok_at.slice(11, 19) : "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>

        {selected && <OrderDetail id={selected} onClose={() => setSelected(null)} />}
        {snap && snap.errors.length > 0 && (
          <section className="card bad">
            <h2>Callback errors</h2>
            <ul>{snap.errors.map((e, i) => <li key={i}><code>{e}</code></li>)}</ul>
          </section>
        )}
      </main>
    </div>
  );
}

function applyToSnapshot(s: Snapshot, n: Notification): Snapshot {
  if (n.type !== "Order") return s;
  const open = { ...s.open_orders };
  const cur = open[n.key];
  if (cur && cur.version >= n.version) return s; // stale or duplicate delivery
  const status = n.data.status as Order["status"];
  if (n.kind !== "delete" && (status === "open" || status === "partial")) {
    open[n.key] = { ...(n.data as unknown as Order), version: n.version };
  } else {
    delete open[n.key];
  }
  return { ...s, open_orders: open };
}

function Stat({ label, value, tone }: { label: string; value: number | undefined; tone?: "bad" | "warn" }) {
  return (
    <div className={`stat ${tone ?? ""}`}>
      <div className="value">{value ?? "—"}</div>
      <div className="label">{label}</div>
    </div>
  );
}

function Actions({
  busy, message, run, orders,
}: { busy: string | null; message: string | null; run: (l: string, f: () => Promise<unknown>) => Promise<void>; orders: Order[] }) {
  const [n, setN] = useState(5);
  const [orderId, setOrderId] = useState("");
  const [qty, setQty] = useState(100);
  useEffect(() => {
    if (!orderId && orders.length > 0) setOrderId(orders[0].order_id);
  }, [orders, orderId]);
  return (
    <section className="card actions">
      <h2>Generate traffic <span className="muted">(writes go to persistence; what you see below came back as webhooks)</span></h2>
      <div className="row">
        <label>seed <input type="number" min={1} max={50} value={n} onChange={(e) => setN(Number(e.target.value))} /> orders</label>
        <button disabled={!!busy} onClick={() => run("seed", () => api.seed(n))}>seed (one transaction)</button>
        <span className="sep" />
        <label>order
          <select value={orderId} onChange={(e) => setOrderId(e.target.value)}>
            {orders.map((o) => <option key={o.order_id} value={o.order_id}>{o.order_id} ({o.qty - (o.filled ?? 0)} left)</option>)}
          </select>
        </label>
        <label>qty <input type="number" min={1} value={qty} onChange={(e) => setQty(Number(e.target.value))} /></label>
        <button disabled={!!busy || !orderId} onClick={() => run(`fill ${orderId}`, () => api.fill(orderId, qty))}>fill</button>
        <button disabled={!!busy || !orderId} onClick={() => run(`cancel ${orderId}`, () => api.cancel(orderId))}>cancel</button>
        {busy && <span className="muted">working: {busy}…</span>}
        {message && <span className="msg">{message}</span>}
      </div>
    </section>
  );
}

function OrdersTable({
  orders, selected, onSelect, busy, run,
}: { orders: Order[]; selected: string | null; onSelect: (id: string) => void; busy: string | null; run: (l: string, f: () => Promise<unknown>) => Promise<void> }) {
  return (
    <table>
      <thead>
        <tr><th>order</th><th>side</th><th>symbol</th><th>qty</th><th>filled</th><th>status</th><th>v</th><th></th></tr>
      </thead>
      <tbody>
        {orders.map((o) => {
          const left = o.qty - (o.filled ?? 0);
          return (
            <tr key={o.order_id} className={selected === o.order_id ? "selected" : ""} onClick={() => onSelect(o.order_id)}>
              <td><code>{o.order_id}</code></td>
              <td className={o.side}>{o.side}</td>
              <td>{o.symbol}</td>
              <td>{o.qty}</td>
              <td>
                <div className="bar"><div style={{ width: `${((o.filled ?? 0) / o.qty) * 100}%` }} /></div>
                {o.filled ?? 0}
              </td>
              <td><span className={`tag ${o.status}`}>{o.status}</span></td>
              <td className="muted">{o.version}</td>
              <td className="rowactions">
                <button disabled={!!busy} onClick={(e) => { e.stopPropagation(); void run(`fill ${o.order_id}`, () => api.fill(o.order_id, Math.max(1, Math.floor(left / 2)))); }}>½ fill</button>
                <button disabled={!!busy} onClick={(e) => { e.stopPropagation(); void run(`fill ${o.order_id}`, () => api.fill(o.order_id, left)); }}>fill all</button>
                <button disabled={!!busy} onClick={(e) => { e.stopPropagation(); void run(`cancel ${o.order_id}`, () => api.cancel(o.order_id)); }}>cancel</button>
              </td>
            </tr>
          );
        })}
        {orders.length === 0 && <tr><td colSpan={8} className="muted">No open orders.</td></tr>}
      </tbody>
    </table>
  );
}

function OrderDetail({ id, onClose }: { id: string; onClose: () => void }) {
  const [detail, setDetail] = useState<Awaited<ReturnType<typeof api.order>> | null>(null);
  const [err, setErr] = useState<string | null>(null);
  useEffect(() => {
    setDetail(null);
    api.order(id).then(setDetail).catch((e: Error) => setErr(e.message));
  }, [id]);
  return (
    <section className="card">
      <h2>
        {id} <span className="muted">read-through from persistence, customer expanded</span>
        <button className="link" onClick={onClose}>close</button>
      </h2>
      {err && <p className="bad">{err}</p>}
      {detail && (
        <>
          <p>
            <strong>{detail._expanded?.customer?.data.name ?? detail.customer}</strong> · {detail.side} {detail.qty} {detail.symbol}
            {detail.limit_price != null && <> @ {detail.limit_price}</>} · {detail.status} · v{detail.version}
          </p>
          <pre>{JSON.stringify(detail, null, 2)}</pre>
        </>
      )}
    </section>
  );
}
