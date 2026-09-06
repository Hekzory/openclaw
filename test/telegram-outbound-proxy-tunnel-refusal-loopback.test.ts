// Root-owned integration may combine the public Telegram plugin with the durable queue runtime.
import { createServer, type Server } from "node:http";
import type { AddressInfo, Socket } from "node:net";
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
import { afterEach, describe, expect, it, vi } from "vitest";
import { getDeliveryQueueEntryStatus } from "../src/infra/delivery-queue-sqlite.js";
import { OUTBOUND_DELIVERY_QUEUE_NAME } from "../src/infra/outbound/delivery-queue-media-staging.js";

const DELIVERY_INTENT_ID = "telegram-loopback-proxy-tunnel-refusal";

type RefusingProxy = {
  url: string;
  connectTargets: string[];
  close: () => Promise<void>;
};

type QueueRow = { recoveryState: string | null; retryCount: number; status: string };

function readQueueRow(stateDir: string): QueueRow | undefined {
  const { db } = openOpenClawStateDatabase({
    env: { ...process.env, OPENCLAW_STATE_DIR: stateDir },
  });
  const row = db
    // sqlite-allow-raw: The proof reads one exact queue owner after each recovery pass.
    .prepare(
      "SELECT status, retry_count, recovery_state FROM delivery_queue_entries WHERE queue_name = ? AND id = ?",
    )
    .get(OUTBOUND_DELIVERY_QUEUE_NAME, DELIVERY_INTENT_ID) as
    | { recovery_state: string | null; retry_count: number; status: string }
    | undefined;
  return row
    ? { recoveryState: row.recovery_state, retryCount: row.retry_count, status: row.status }
    : undefined;
}

// A forward proxy whose CONNECT handler always answers 503, the shape a stalled
// upstream (for example an ssh -D hop behind an HTTP proxy) produces. The TLS
// session to api.telegram.org never starts, so nothing can reach Telegram.
async function startRefusingProxy(): Promise<RefusingProxy> {
  const connectTargets: string[] = [];
  const sockets = new Set<Socket>();
  const server: Server = createServer((_request, response) => {
    response.writeHead(405, { "content-length": "0" });
    response.end();
  });
  server.on("connect", (request, socket) => {
    connectTargets.push(request.url ?? "");
    socket.end(
      "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    );
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
    connectTargets,
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

describe("Telegram proxy tunnel refusal over the real proxy transport", () => {
  afterEach(() => {
    closeOpenClawAgentDatabasesForTest();
    closeOpenClawStateDatabaseForTest();
    resetGlobalHookRunner();
    resetPluginRuntimeStateForTest();
    setActivePluginRegistry(createEmptyPluginRegistry());
  });

  it("keeps a durable send replayable while the proxy refuses the CONNECT tunnel", async () => {
    const proxy = await startRefusingProxy();
    try {
      const { telegramPlugin } = await import("../extensions/telegram/api.js");
      const cfg = {
        channels: {
          telegram: {
            botToken: "123456:loopback",
            proxy: proxy.url,
          },
        },
      } satisfies OpenClawConfig;
      setActivePluginRegistry(
        createTestRegistry([{ pluginId: "telegram", plugin: telegramPlugin, source: "test" }]),
      );

      await withStateDirEnv("openclaw-telegram-proxy-tunnel-loopback-", async ({ stateDir }) => {
        try {
          const staged = await sendDurableMessageBatch({
            cfg,
            channel: "telegram",
            to: "123",
            accountId: "default",
            durability: "required",
            deliveryIntentId: DELIVERY_INTENT_ID,
            completionRetention: {
              idPrefix: "telegram-loopback-",
              maxAgeMs: 60_000,
              maxEntries: 10,
            },
            maxRetries: 10,
            payloads: [{ text: "reply that must survive a proxy tunnel refusal" }],
            deps: {
              telegram: async () => {
                throw new PlatformMessageNotDispatchedError(
                  "staged before transport for recovery proof",
                  { cause: new Error("loopback proxy not released yet") },
                );
              },
            },
          });
          expect(staged.status).toBe("failed");
          expect(proxy.connectTargets).toHaveLength(0);
          expect(
            getDeliveryQueueEntryStatus(OUTBOUND_DELIVERY_QUEUE_NAME, DELIVERY_INTENT_ID, stateDir),
          ).toBe("pending");
          const stagedRow = readQueueRow(stateDir);
          expect(stagedRow?.status).toBe("pending");
          const stagedRetryCount = stagedRow?.retryCount ?? 0;

          const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
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

          await drain("Telegram loopback proxy tunnel recovery");

          // Every attempt ended at the proxy hop: the in-process retry ran, each
          // attempt targeted the Bot API tunnel, and nothing was accepted.
          const firstPassConnects = proxy.connectTargets.length;
          expect(firstPassConnects).toBeGreaterThanOrEqual(2);
          expect(new Set(proxy.connectTargets)).toEqual(new Set(["api.telegram.org:443"]));
          const afterFirstPass = readQueueRow(stateDir);
          expect(afterFirstPass).toMatchObject({
            retryCount: stagedRetryCount + 1,
            status: "pending",
          });
          // The typed no-send proof keeps the entry replayable instead of parking
          // it as an ambiguous send that recovery must refuse to replay.
          expect(afterFirstPass?.recoveryState).not.toBe("unknown_after_send");
          for (const call of log.warn.mock.calls) {
            expect(String(call[0])).not.toContain("unknown_after_send");
          }

          closeOpenClawAgentDatabasesForTest();
          closeOpenClawStateDatabaseForTest();
          expect(
            getDeliveryQueueEntryStatus(OUTBOUND_DELIVERY_QUEUE_NAME, DELIVERY_INTENT_ID, stateDir),
          ).toBe("pending");

          await drain("Telegram loopback post-restart proxy tunnel recovery");
          expect(proxy.connectTargets.length).toBeGreaterThan(firstPassConnects);
          const afterSecondPass = readQueueRow(stateDir);
          expect(afterSecondPass).toMatchObject({
            retryCount: stagedRetryCount + 2,
            status: "pending",
          });
          expect(afterSecondPass?.recoveryState).not.toBe("unknown_after_send");

          console.log(
            `[telegram proxy-tunnel proof] ${JSON.stringify({
              queueStatus: afterSecondPass?.status,
              recoveryState: afterSecondPass?.recoveryState,
              retryCountAfterTwoRecoveryPasses: afterSecondPass?.retryCount,
              connectAttempts: proxy.connectTargets.length,
              proxyResponse: 503,
              classification: "request not started (proxy tunnel never opened)",
              transport: "grammY Bot API HTTPS via CONNECT to 127.0.0.1:<redacted>",
            })}`,
          );
        } finally {
          closeOpenClawAgentDatabasesForTest();
          closeOpenClawStateDatabaseForTest();
        }
      });
    } finally {
      await proxy.close();
    }
  });
});
