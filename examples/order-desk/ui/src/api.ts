// Thin client for the dashboard's /api endpoints. The dashboard proxies pub/sub admin calls,
// so the browser never needs the dbpersist API key.

export type Order = {
  version: number;
  order_id: string;
  customer: string;
  symbol: string;
  side: "buy" | "sell";
  qty: number;
  filled?: number;
  limit_price?: number | null;
  status: "open" | "partial" | "filled" | "cancelled";
  notes?: string[];
};

export type Notification = {
  at: string;
  kind: "insert" | "update" | "delete";
  type: string;
  key: string;
  version: number;
  data: Record<string, unknown>;
  changed?: string[];
};

export type Snapshot = {
  subscriber: string;
  subscriber_name: string;
  webhook_url: string;
  open_orders: Record<string, Order>;
  status_changes: { order: string; status: string }[];
  recent: Notification[];
  errors: string[];
};

export type Stats = {
  received: number;
  matched: number;
  delivered: number;
  failed: number;
  subscribers: number;
  subscriptions: number;
};

export type ClientInfo = {
  id: string;
  name: string;
  webhook_url: string;
  subscriptions: number;
  pending: number;
  failed: number;
  last_seen_at: string;
  last_delivery_ok_at: string | null;
};

export type Subscription = {
  id: string;
  type_name: string;
  filter: unknown | null;
  keys: string[] | null;
  by: string;
};

const BASE = "/api";

async function get<T>(path: string): Promise<T> {
  const r = await fetch(`${BASE}${path}`);
  if (!r.ok) throw new Error(`${path}: ${r.status}`);
  return (await r.json()) as T;
}

async function post<T>(path: string, body: unknown): Promise<T> {
  const r = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error((data as { detail?: string }).detail ?? `${path}: ${r.status}`);
  return data as T;
}

export const api = {
  state: () => get<Snapshot>("/state"),
  stats: () => get<Stats>("/pubsub/stats"),
  clients: () => get<ClientInfo[]>("/pubsub/clients"),
  subscriptions: () => get<Subscription[]>("/pubsub/subscriptions"),
  order: (id: string) => get<Order & { _expanded?: { customer?: { data: { name: string } } } }>(`/orders/${id}`),
  seed: (orders: number) => post<{ ok: boolean }>("/actions/seed", { orders }),
  fill: (order_id: string, qty: number) => post<{ version: number; status: string }>("/actions/fill", { order_id, qty }),
  cancel: (order_id: string) => post<{ version: number; status: string }>("/actions/cancel", { order_id }),
  eventsUrl: `${BASE}/events`,
};
