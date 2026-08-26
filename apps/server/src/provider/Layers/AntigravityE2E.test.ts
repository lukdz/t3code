import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import {
  AntigravitySettings,
  ProviderDriverKind,
  ThreadId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";
import * as Chunk from "effect/Chunk";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import { makeAntigravityAdapter } from "./AntigravityAdapter.ts";

const decodeAntigravitySettings = Schema.decodeSync(AntigravitySettings);

it.layer(NodeServices.layer)("Antigravity E2E Live Integration", (it) => {
  it.effect(
    "executes live turn with real agy binary and streams response",
    () =>
      Effect.gen(function* () {
        const settings = decodeAntigravitySettings({
          enabled: true,
          binaryPath: "agy",
        });

        const adapter = yield* makeAntigravityAdapter(settings);
        const threadId = ThreadId.make("thread-live-e2e-" + Date.now());

        const session = yield* adapter.startSession({
          threadId,
          cwd: "/home/lukdz/t3code",
          runtimeMode: "full-access",
        });

        expect(session.provider).toBe(ProviderDriverKind.make("antigravity"));
        expect(session.status).toBe("ready");

        const eventsFiber = yield* Effect.forkChild(
          adapter.streamEvents.pipe(
            Stream.filter((e) => e.threadId === threadId),
            Stream.takeUntil((e) => e.type === "turn.completed"),
            Stream.runCollect,
          ),
        );

        yield* Effect.sleep("50 millis");

        const turnResult = yield* adapter.sendTurn({
          threadId,
          input: "Reply with exactly the word 'PONG' and nothing else.",
        });

        expect(turnResult.turnId).toBeDefined();

        const eventsChunk = yield* Fiber.join(eventsFiber);
        const events = Array.isArray(eventsChunk) ? eventsChunk : Chunk.toArray(eventsChunk as any);

        const eventTypes = events.map((e: ProviderRuntimeEvent) => e.type);
        expect(eventTypes).toContain("turn.started");
        expect(eventTypes).toContain("turn.completed");

        const deltas = events
          .filter((e: any) => e.type === "content.delta")
          .map((e: any) => e.payload.delta)
          .join("");

        expect(deltas.toUpperCase()).toContain("PONG");

        yield* adapter.stopSession(threadId);
      }).pipe(TestClock.withLive),
    120_000,
  );
});
