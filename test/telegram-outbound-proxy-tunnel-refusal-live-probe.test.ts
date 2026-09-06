// LIVE PROBE (not part of the PR): real Telegram Bot API through a switchable
// CONNECT proxy. Records what the durable queue and undici do when the proxy
// refuses the tunnel and after it recovers. Gated by env; skipped otherwise.
import diagnosticsChannel from "node:diagnostics_channel";
import { createServer, type Server } from "node:http";
import { connect as netConnect, type AddressInfo, type Socket } from "node:net";
import { sendDurableMessageBatch } from "openclaw/plugin-sdk/channel-outbound";
import {
  createEmptyPluginRegistry,
  createTestRegistry,
  resetPluginRuntimeStateForTest,
  resetGlobalHookRunner,
  setActivePluginRegistry,
} from "openclaw/plugin-sdk/channel-test-helpers";
import type { OpenClawConfig } from "openclaw/plugin-sdk/config-contracts";
import { drainPendingDeliveries } from "openclaw/plugin-sdk/delivery-queue-runtime";
import { PlatformMessageNotDispatchedError } from "openclaw/plugin-sdk/error-runtime";
import {
  closeOpenClawAgentDatabasesForTest,
  closeOpenClawStateDatabaseForTest,
  openOpenClawStateDatabase,
} from "openclaw/plugin-sdk/sqlite-runtime-testing";
import { withStateDirEnv } from "openclaw/plugin-sdk/test-env";
import { afterEach, describe, expect, it } from "vitest";
import { OUTBOUND_DELIVERY_QUEUE_NAME } from "../src/infra/outbound/delivery-queue-media-staging.js";
import { initializeGlobalHookRunner } from "../src/plugins/hook-runner-global.js";
import { addTestHook } from "../src/plugins/hooks.test-helpers.js";

const BOT_TOKEN = process.env.TELEGRAM_PROBE_BOT_TOKEN ?? "";
const CHAT_ID = process.env.TELEGRAM_PROBE_CHAT_ID ?? "";
const SCENARIO = process.env.TELEGRAM_PROBE_SCENARIO ?? "refuse-then-recover";
const LABEL = process.env.TELEGRAM_PROBE_LABEL ?? "unlabeled";
const ENABLED = BOT_TOKEN.length > 0 && CHAT_ID.length > 0;

type ProxyMode = "refuse" | "forward" | "accept-then-reset";
type ConnectRecord = { at: string; mode: ProxyMode; target: string; outcome: string };
type LogLine = { at: string; level: string; text: string };
type ConnectError = { at: string; name?: string; code?: string; message?: string };

function redact(text: string): string {
  return text.split(BOT_TOKEN).join("<bot-token>").split(CHAT_ID).join("<chat-id>");
}

function stamp(): string {
  return new Date().toISOString();
}

async function startSwitchableProxy() {
  const records: ConnectRecord[] = [];
  const sockets = new Set<Socket>();
  let mode: ProxyMode = "refuse";
  const server: Server = createServer((_request, response) => {
    response.writeHead(405, { "content-length": "0" });
    response.end();
  });
  server.on("connect", (request, clientSocket: Socket, head: Buffer) => {
    const target = request.url ?? "";
    const record: ConnectRecord = { at: stamp(), mode, target, outcome: "" };
    records.push(record);
    if (mode === "refuse") {
      record.outcome = "503 Service Unavailable (tunnel refused)";
      clientSocket.end(
        "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
      );
      return;
    }
    if (mode === "accept-then-reset") {
      record.outcome = "200 Connection Established, then socket destroyed before TLS";
      clientSocket.write("HTTP/1.1 200 Connection Established\r\n\r\n", () => {
        clientSocket.destroy();
      });
      return;
    }
    const [host, portText] = target.split(":");
    const upstream = netConnect(Number(portText ?? "443"), host ?? "", () => {
      record.outcome = "200 Connection Established (tunneled to upstream)";
      clientSocket.write("HTTP/1.1 200 Connection Established\r\n\r\n");
      if (head?.length) {
        upstream.write(head);
      }
      upstream.pipe(clientSocket);
      clientSocket.pipe(upstream);
    });
    sockets.add(upstream);
    upstream.on("error", (error: NodeJS.ErrnoException) => {
      record.outcome = `upstream connect error ${error.code ?? error.message}`;
      clientSocket.destroy();
    });
    upstream.on("close", () => {
      sockets.delete(upstream);
      clientSocket.destroy();
    });
    clientSocket.on("error", () => upstream.destroy());
    clientSocket.on("close", () => upstream.destroy());
  });
  server.on("connection", (socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const { port } = server.address() as AddressInfo;
  return {
    url: `http://127.0.0.1:${port}`,
    records,
    setMode(next: ProxyMode) {
      mode = next;
    },
    close: async () => {
      for (const socket of sockets) {
        socket.destroy();
      }
      await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
    },
  };
}

function readQueueRow(stateDir: string, id: string): Record<string, unknown> | undefined {
  const { db } = openOpenClawStateDatabase({
    env: { ...process.env, OPENCLAW_STATE_DIR: stateDir },
  });
  return (
    db
      // sqlite-allow-raw: live probe reads the exact queue owner row.
      .prepare("SELECT * FROM delivery_queue_entries WHERE queue_name = ? AND id = ?")
      .get(OUTBOUND_DELIVERY_QUEUE_NAME, id) as Record<string, unknown> | undefined
  );
}

function summarizeRow(row: Record<string, unknown> | undefined) {
  if (!row) {
    return undefined;
  }
  const pick = (key: string) => (key in row ? row[key] : undefined);
  return {
    status: pick("status"),
    retryCount: pick("retry_count"),
    attemptCount: pick("attempt_count"),
    recoveryState: pick("recovery_state"),
    lastError:
      typeof pick("last_error") === "string"
        ? redact(String(pick("last_error"))).slice(0, 200)
        : pick("last_error"),
  };
}

// Observation only: records what the real Telegram outbound adapter returns.
function observeOutbound<T extends object>(
  target: T,
  sink: Array<{ method: string; result: unknown }>,
): T {
  return new Proxy(target, {
    get(obj, prop, receiver) {
      const value = Reflect.get(obj, prop, receiver);
      if (typeof value !== "function") {
        return value;
      }
      return (...args: unknown[]) => {
        const out = value.apply(obj, args);
        if (out && typeof (out as Promise<unknown>).then === "function") {
          return (out as Promise<unknown>).then((result) => {
            sink.push({ method: String(prop), result });
            return result;
          });
        }
        return out;
      };
    },
  });
}

describe.skipIf(!ENABLED)("LIVE Telegram proxy tunnel probe", () => {
  afterEach(() => {
    closeOpenClawAgentDatabasesForTest();
    closeOpenClawStateDatabaseForTest();
    resetGlobalHookRunner();
    resetPluginRuntimeStateForTest();
    setActivePluginRegistry(createEmptyPluginRegistry());
  });

  it(`scenario ${SCENARIO} [${LABEL}]`, { timeout: 180_000 }, async () => {
    const connectErrors: ConnectError[] = [];
    const onConnectError = (message: unknown) => {
      const error = (message as { error?: { name?: string; code?: string; message?: string } })
        ?.error;
      connectErrors.push({
        at: stamp(),
        name: error?.name,
        code: error?.code,
        message: error?.message ? redact(error.message) : undefined,
      });
    };
    diagnosticsChannel.subscribe("undici:client:connectError", onConnectError);
    const proxy = await startSwitchableProxy();
    const adapterResults: Array<{ method: string; result: unknown }> = [];
    const logLines: LogLine[] = [];
    const sentEvents: Array<{ at: string; event: unknown; ctx: unknown }> = [];
    const log = {
      info: (text: unknown) =>
        logLines.push({ at: stamp(), level: "info", text: redact(String(text)) }),
      warn: (text: unknown) =>
        logLines.push({ at: stamp(), level: "warn", text: redact(String(text)) }),
      error: (text: unknown) =>
        logLines.push({ at: stamp(), level: "error", text: redact(String(text)) }),
    };
    const nonce = `${LABEL}-${Date.now().toString(36)}`;
    const entryId = `telegram-live-proxy-probe-${nonce}`;
    const timeline: Array<Record<string, unknown>> = [];
    try {
      const { telegramPlugin } = await import("../extensions/telegram/api.js");
      const plugin = {
        ...telegramPlugin,
        outbound: telegramPlugin.outbound
          ? observeOutbound(telegramPlugin.outbound, adapterResults)
          : telegramPlugin.outbound,
      };
      const cfg = {
        channels: { telegram: { botToken: BOT_TOKEN, proxy: proxy.url } },
      } satisfies OpenClawConfig;
      const registry = createTestRegistry([{ pluginId: "telegram", plugin, source: "test" }]);
      // Observation only: the queue emits message_sent with the Telegram message id
      // once the replay is accepted by the Bot API.
      addTestHook({
        registry,
        pluginId: "live-probe-observer",
        hookName: "message_sent",
        handler: (event: unknown, ctx: unknown) => {
          sentEvents.push({
            at: stamp(),
            event: JSON.parse(redact(JSON.stringify(event ?? null))),
            ctx: JSON.parse(redact(JSON.stringify(ctx ?? null))),
          });
        },
      });
      setActivePluginRegistry(registry);
      initializeGlobalHookRunner(registry);

      await withStateDirEnv("openclaw-telegram-live-proxy-probe-", async ({ stateDir }) => {
        try {
          const staged = await sendDurableMessageBatch({
            cfg,
            channel: "telegram",
            to: CHAT_ID,
            accountId: "default",
            durability: "required",
            deliveryIntentId: entryId,
            completionRetention: { idPrefix: "telegram-live-", maxAgeMs: 600_000, maxEntries: 10 },
            maxRetries: 10,
            payloads: [
              {
                text: `[openclaw live probe ${nonce}] scenario=${SCENARIO} — this reply was queued while the HTTP proxy refused the CONNECT tunnel`,
              },
            ],
            deps: {
              telegram: async () => {
                throw new PlatformMessageNotDispatchedError(
                  "staged before transport for live probe",
                  {
                    cause: new Error("proxy still refusing"),
                  },
                );
              },
            },
          });
          const stagedRow = readQueueRow(stateDir, entryId);
          timeline.push({
            step: "staged",
            at: stamp(),
            stagedStatus: staged.status,
            row: summarizeRow(stagedRow),
            rowColumns: stagedRow ? Object.keys(stagedRow) : [],
            connects: proxy.records.length,
          });

          const drain = (logLabel: string) =>
            drainPendingDeliveries({
              drainKey: "telegram:default",
              logLabel,
              cfg,
              stateDir,
              log,
              selectEntry: (entry) => ({
                match: entry.channel === "telegram",
                bypassBackoff: true,
              }),
            });
          const restart = () => {
            closeOpenClawAgentDatabasesForTest();
            closeOpenClawStateDatabaseForTest();
          };
          const snapshot = (step: string, extra: Record<string, unknown> = {}) => {
            timeline.push({
              step,
              at: stamp(),
              proxyMode: proxy.records.at(-1)?.mode,
              row: summarizeRow(readQueueRow(stateDir, entryId)),
              connectsSoFar: proxy.records.length,
              connectErrorsSoFar: connectErrors.length,
              ...extra,
            });
          };

          if (SCENARIO === "refuse-then-recover") {
            proxy.setMode("refuse");
            await drain("live probe pass 1 (proxy refusing CONNECT)");
            snapshot("pass1-refused");
            restart();
            await drain("live probe pass 2 after restart (proxy still refusing)");
            snapshot("pass2-refused-after-restart");
            restart();
            proxy.setMode("forward");
            await drain("live probe pass 3 (proxy recovered)");
            snapshot("pass3-recovered");
          } else if (SCENARIO === "accept-then-reset") {
            proxy.setMode("accept-then-reset");
            await drain("live probe control pass 1 (tunnel accepted, socket reset)");
            snapshot("pass1-accepted-then-reset");
            restart();
            proxy.setMode("forward");
            await drain("live probe control pass 2 (proxy healthy)");
            snapshot("pass2-healthy");
          } else {
            throw new Error(`unknown scenario ${SCENARIO}`);
          }

          const proof = {
            label: LABEL,
            scenario: SCENARIO,
            nonce,
            transport:
              "grammY Bot API HTTPS via CONNECT through a local HTTP proxy (channels.telegram.proxy)",
            botApiHost: "api.telegram.org",
            timeline,
            proxyConnects: proxy.records.map((r) => ({ ...r, target: redact(r.target) })),
            undiciConnectErrors: connectErrors,
            adapterResults: adapterResults.map((r) => ({
              method: r.method,
              result: JSON.parse(redact(JSON.stringify(r.result ?? null))),
            })),
            drainLog: logLines,
            messageSentEvents: sentEvents,
          };
          console.log(`[telegram live proxy probe] ${JSON.stringify(proof, null, 2)}`);
          expect(proxy.records.length).toBeGreaterThan(0);
        } finally {
          closeOpenClawAgentDatabasesForTest();
          closeOpenClawStateDatabaseForTest();
        }
      });
    } finally {
      diagnosticsChannel.unsubscribe("undici:client:connectError", onConnectError);
      await proxy.close();
    }
  });
});
