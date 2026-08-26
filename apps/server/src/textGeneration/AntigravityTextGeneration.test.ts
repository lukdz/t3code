import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import { AntigravitySettings, ProviderInstanceId } from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import { makeAntigravityTextGeneration } from "./AntigravityTextGeneration.ts";

const decodeAntigravitySettings = Schema.decodeSync(AntigravitySettings);

function makeFakeAgyBinary(dir: string, mockResponseJson: string) {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const binPath = path.join(dir, "agy");

    yield* fs.writeFileString(
      binPath,
      ["#!/bin/sh", `echo '${mockResponseJson}'`, "exit 0"].join("\n"),
    );
    yield* fs.chmod(binPath, 0o755);
    return binPath;
  });
}

it.layer(NodeServices.layer)("makeAntigravityTextGeneration", (it) => {
  it.effect("generates a commit message using mocked agy binary", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-agy-textgen-" });
      const binaryPath = yield* makeFakeAgyBinary(
        dir,
        JSON.stringify({
          subject: "feat: add antigravity provider support",
          body: "Implements the Antigravity backend adapter and snapshot probe.",
        }),
      );

      const settings = decodeAntigravitySettings({
        enabled: true,
        binaryPath,
      });

      const textGen = yield* makeAntigravityTextGeneration(settings);
      const result = yield* textGen.generateCommitMessage({
        cwd: dir,
        branch: "main",
        stagedSummary: "modified files",
        stagedPatch: "diff --git a/file b/file",
        modelSelection: createModelSelection({
          instanceId: ProviderInstanceId.make("antigravity"),
          model: "gemini-3.7-flash-low",
        }),
      });

      expect(result.subject).toBe("feat: add antigravity provider support");
      expect(result.body).toBe("Implements the Antigravity backend adapter and snapshot probe.");
    }),
  );

  it.effect("generates a thread title using mocked agy binary", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-agy-textgen-" });
      const binaryPath = yield* makeFakeAgyBinary(
        dir,
        JSON.stringify({
          title: "Setup Antigravity Backend",
        }),
      );

      const settings = decodeAntigravitySettings({
        enabled: true,
        binaryPath,
      });

      const textGen = yield* makeAntigravityTextGeneration(settings);
      const result = yield* textGen.generateThreadTitle({
        cwd: dir,
        message: "Can we add Antigravity CLI support to T3 Code?",
        modelSelection: createModelSelection({
          instanceId: ProviderInstanceId.make("antigravity"),
          model: "gemini-3.7-flash-low",
        }),
      });

      expect(result.title).toBe("Setup Antigravity Backend");
    }),
  );
});
