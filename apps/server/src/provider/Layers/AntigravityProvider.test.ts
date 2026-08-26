import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { AntigravitySettings } from "@t3tools/contracts";

import {
  buildInitialAntigravityProviderSnapshot,
  checkAntigravityProviderStatus,
  parseAntigravityModelsOutput,
} from "./AntigravityProvider.ts";

const decodeAntigravitySettings = Schema.decodeSync(AntigravitySettings);

describe("parseAntigravityModelsOutput", () => {
  it("parses TSV model lines into ServerProviderModel entries", () => {
    const raw = [
      "gemini-3.7-flash-high\tGemini 3.7 Flash (High)",
      "gemini-3.1-pro-high\tGemini 3.1 Pro (High)",
      "claude-sonnet-4-6\tClaude Sonnet 4.6 (Thinking)",
    ].join("\n");

    const models = parseAntigravityModelsOutput(raw);
    expect(models).toHaveLength(3);
    expect(models[0]).toEqual({
      slug: "gemini-3.7-flash-high",
      name: "Gemini 3.7 Flash (High)",
      isCustom: false,
      capabilities: { optionDescriptors: [] },
    });
    expect(models[1]?.slug).toBe("gemini-3.1-pro-high");
    expect(models[2]?.name).toBe("Claude Sonnet 4.6 (Thinking)");
  });

  it("handles duplicate slugs gracefully", () => {
    const raw = [
      "gemini-3.7-flash-high\tGemini 3.7 Flash (High)",
      "gemini-3.7-flash-high\tDuplicate Name",
    ].join("\n");

    const models = parseAntigravityModelsOutput(raw);
    expect(models).toHaveLength(1);
    expect(models[0]?.name).toBe("Gemini 3.7 Flash (High)");
  });
});

describe("buildInitialAntigravityProviderSnapshot", () => {
  it.effect("returns a disabled snapshot when settings.enabled is false", () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialAntigravityProviderSnapshot(
        decodeAntigravitySettings({ enabled: false }),
      );
      expect(snapshot.enabled).toBe(false);
      expect(snapshot.status).toBe("disabled");
      expect(snapshot.installed).toBe(false);
      expect(snapshot.message).toContain("disabled");
    }),
  );

  it.effect("returns a disabled snapshot by default — Antigravity is opt-in", () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialAntigravityProviderSnapshot(
        decodeAntigravitySettings({}),
      );
      expect(snapshot.enabled).toBe(false);
      expect(snapshot.status).toBe("disabled");
    }),
  );

  it.effect("returns a pending snapshot when enabled", () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialAntigravityProviderSnapshot(
        decodeAntigravitySettings({ enabled: true }),
      );
      expect(snapshot.enabled).toBe(true);
      expect(snapshot.installed).toBe(true);
      expect(snapshot.status).toBe("warning");
      expect(snapshot.version).toBeNull();
      expect(snapshot.message).toContain("Checking Antigravity");
      expect(snapshot.requiresNewThreadForModelChange).toBe(true);
    }),
  );
});

it.layer(NodeServices.layer)("checkAntigravityProviderStatus", (it) => {
  it.effect("reports the binary as missing when the binary path does not resolve", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkAntigravityProviderStatus(
        decodeAntigravitySettings({
          enabled: true,
          binaryPath: "/definitely/not/installed/agy-binary",
        }),
      );
      expect(snapshot.enabled).toBe(true);
      expect(snapshot.installed).toBe(false);
      expect(snapshot.status).toBe("error");
      expect(snapshot.message).toMatch(/not installed|not on PATH|Failed to execute/);
    }),
  );

  it.effect("discovers version and models when a mock agy script is executable", () =>
    Effect.gen(function* () {
      const snapshot = yield* Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-agy-test-" });
          const agyPath = path.join(dir, "agy");
          yield* fs.writeFileString(
            agyPath,
            [
              "#!/bin/sh",
              'if [ "$1" = "--version" ]; then',
              '  echo "1.1.21"',
              "  exit 0",
              'elif [ "$1" = "models" ]; then',
              '  printf "gemini-3.7-flash-high\\tGemini 3.7 Flash\\ngemini-3.1-pro-high\\tGemini 3.1 Pro\\n"',
              "  exit 0",
              "fi",
              "exit 1",
            ].join("\n"),
          );
          yield* fs.chmod(agyPath, 0o755);

          return yield* checkAntigravityProviderStatus(
            decodeAntigravitySettings({
              enabled: true,
              binaryPath: agyPath,
            }),
          );
        }),
      );

      expect(snapshot.enabled).toBe(true);
      expect(snapshot.installed).toBe(true);
      expect(snapshot.status).toBe("ready");
      expect(snapshot.version).toBe("1.1.21");
      expect(snapshot.auth.status).toBe("authenticated");
      expect(snapshot.models.map((m) => m.slug)).toEqual([
        "gemini-3.7-flash-high",
        "gemini-3.1-pro-high",
      ]);
    }),
  );
});
