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
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import { makeAntigravityAdapter } from "./AntigravityAdapter.ts";

const decodeAntigravitySettings = Schema.decodeSync(AntigravitySettings);

it.layer(NodeServices.layer)("AntigravityAdapter", (it) => {
  it.effect("starts session and executes a turn with NDJSON event streaming", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-agy-adapter-" });
      const mockScriptPath = path.join(dir, "mock-agy.sh");

      // Exact structure matching agy output
      const mockLines = [
        JSON.stringify({
          event: "init",
          conversation_id: "conv-12345",
          init: {
            cwd: dir,
            tools: ["view_file", "write_to_file"],
            permission_mode: "always-proceed",
          },
        }),
        JSON.stringify({
          event: "step_update",
          step_update: {
            conversation_id: "conv-12345",
            step_index: 0,
            state: "DONE",
            step_type: "user_input",
          },
        }),
        JSON.stringify({
          event: "step_update",
          step_update: {
            conversation_id: "conv-12345",
            step_index: 1,
            state: "DONE",
            step_type: "agent_response",
            text_delta: "Hello from ",
          },
        }),
        JSON.stringify({
          event: "step_update",
          step_update: {
            conversation_id: "conv-12345",
            step_index: 1,
            state: "DONE",
            step_type: "agent_response",
            text_delta: "Antigravity!",
          },
        }),
        JSON.stringify({
          event: "step_update",
          step_update: {
            conversation_id: "conv-12345",
            step_index: 2,
            state: "ACTIVE",
            step_type: "tool",
            tool_name: "view_file",
            tool_info: {
              name: "view_file",
              parameters: { AbsolutePath: "/test/file.ts" },
            },
          },
        }),
        JSON.stringify({
          event: "step_update",
          step_update: {
            conversation_id: "conv-12345",
            step_index: 2,
            state: "DONE",
            step_type: "tool",
            tool_name: "view_file",
            tool_info: {
              name: "view_file",
              parameters: { AbsolutePath: "/test/file.ts" },
              output: "file contents",
            },
          },
        }),
        JSON.stringify({
          event: "step_update",
          step_update: {
            conversation_id: "conv-12345",
            step_index: 3,
            state: "DONE",
            step_type: "tool",
            subagent_info: {
              conversation_id: "subagent-999",
              role: "Code Reviewer",
              status: "running",
            },
          },
        }),
        JSON.stringify({
          event: "result",
          result: {
            conversation_id: "conv-12345",
            status: "SUCCESS",
            response: "Hello from Antigravity!",
            usage: {
              input_tokens: 100,
              output_tokens: 50,
              total_tokens: 150,
            },
          },
        }),
      ];

      yield* fs.writeFileString(
        mockScriptPath,
        ["#!/bin/sh", ...mockLines.map((l) => `echo '${l}'`), "exit 0"].join("\n"),
      );
      yield* fs.chmod(mockScriptPath, 0o755);

      const settings = decodeAntigravitySettings({
        enabled: true,
        binaryPath: mockScriptPath,
      });

      const adapter = yield* makeAntigravityAdapter(settings);
      const threadId = ThreadId.make("thread-test-1");

      const session = yield* adapter.startSession({
        threadId,
        cwd: dir,
        runtimeMode: "full-access",
      });

      expect(session.provider).toBe(ProviderDriverKind.make("antigravity"));
      expect(session.status).toBe("ready");

      const eventsFiber = yield* Effect.forkChild(
        adapter.streamEvents.pipe(Stream.take(7), Stream.runCollect),
      );

      yield* Effect.sleep("20 millis");

      const turnResult = yield* adapter.sendTurn({
        threadId,
        input: "Say hello",
      });

      expect(turnResult.turnId).toBeDefined();
      expect(turnResult.resumeCursor).toEqual({
        schemaVersion: 1,
        conversationId: "conv-12345",
      });

      const eventsChunk = yield* Fiber.join(eventsFiber);
      const events = Array.isArray(eventsChunk) ? eventsChunk : Chunk.toArray(eventsChunk as any);

      const eventTypes = events.map((e) => e.type);
      expect(eventTypes).toContain("turn.started");
      expect(eventTypes).toContain("content.delta");
      expect(eventTypes).toContain("item.started");
      expect(eventTypes).toContain("item.completed");
      expect(eventTypes).toContain("task.started");
      expect(eventTypes).toContain("turn.completed");

      const textDeltas = events
        .filter((e) => e.type === "content.delta")
        .map((e: any) => e.payload.delta)
        .join("");
      expect(textDeltas).toBe("Hello from Antigravity!");

      const toolItem = events.find((e) => e.type === "item.started") as any;
      expect(toolItem.payload.itemType).toBe("dynamic_tool_call");
      expect(toolItem.payload.title).toBe("view_file");

      const taskItem = events.find((e) => e.type === "task.started") as any;
      expect(taskItem.payload.agentId).toBe("subagent-999");
      expect(taskItem.payload.role).toBe("Code Reviewer");
      expect(taskItem.payload.timelineBypass).toBe(true);

      const turnCompleted = events.find((e) => e.type === "turn.completed") as any;
      expect(turnCompleted.payload.state).toBe("completed");
      expect(turnCompleted.payload.usage.totalTokens).toBe(150);

      yield* adapter.stopSession(threadId);
    }).pipe(TestClock.withLive),
  );

  it.effect("handles turn interruption cleanly", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-agy-adapter-abort-" });
      const mockScriptPath = path.join(dir, "mock-agy-sleep.sh");

      yield* fs.writeFileString(
        mockScriptPath,
        [
          "#!/bin/sh",
          'echo \'{"event":"init","conversation_id":"conv-sleep"}\'',
          "exec sleep 30",
        ].join("\n"),
      );
      yield* fs.chmod(mockScriptPath, 0o755);

      const settings = decodeAntigravitySettings({
        enabled: true,
        binaryPath: mockScriptPath,
      });

      const adapter = yield* makeAntigravityAdapter(settings);
      const threadId = ThreadId.make("thread-test-abort");

      yield* adapter.startSession({
        threadId,
        cwd: dir,
        runtimeMode: "full-access",
      });

      const turnFiber = yield* Effect.forkChild(
        adapter.sendTurn({
          threadId,
          input: "Run long task",
        }),
      );

      yield* Effect.sleep("50 millis");
      yield* adapter.interruptTurn(threadId);
      yield* Fiber.join(turnFiber);

      const session = (yield* adapter.listSessions())[0];
      expect(session?.status).toBe("ready");

      yield* adapter.stopSession(threadId);
    }).pipe(TestClock.withLive),
  );
});
