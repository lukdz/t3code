import {
  ApprovalRequestId,
  type AntigravitySettings,
  EventId,
  type ProviderApprovalDecision,
  type ProviderRuntimeEvent,
  type ProviderSession,
  type ProviderUserInputAnswers,
  ProviderDriverKind,
  ProviderInstanceId,
  type ThreadId,
  TurnId,
  type ProviderSendTurnInput,
  type ProviderSessionStartInput,
  type ProviderTurnStartResult,
  type CanonicalItemType,
  RuntimeItemId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import { collectUint8StreamText } from "../../stream/collectUint8StreamText.ts";

import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import { type AntigravityAdapterShape } from "../Services/AntigravityAdapter.ts";

const PROVIDER = ProviderDriverKind.make("antigravity");
const ANTIGRAVITY_RESUME_VERSION = 1 as const;

export interface AntigravityAdapterLiveOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly instanceId?: ProviderInstanceId;
}

interface AntigravitySessionContext {
  readonly threadId: ThreadId;
  conversationId: string | null;
  session: ProviderSession;
  readonly scope: Scope.Closeable;
  activeProcess: ChildProcess.ChildProcess | null;
  activeTurnId: TurnId | undefined;
  turns: Array<{ id: TurnId; items: Array<unknown> }>;
  knownSubagents: Set<string>;
  stopped: boolean;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseAntigravityResume(raw: unknown): { conversationId: string } | undefined {
  if (!isRecord(raw)) return undefined;
  if (raw.schemaVersion !== ANTIGRAVITY_RESUME_VERSION) return undefined;
  if (typeof raw.conversationId !== "string" || !raw.conversationId.trim()) return undefined;
  return { conversationId: raw.conversationId.trim() };
}

function classifyToolName(name: string): CanonicalItemType {
  switch (name) {
    case "write_to_file":
    case "replace_file_content":
      return "file_change";
    case "run_command":
      return "command_execution";
    case "search_web":
    case "read_url_content":
      return "web_search";
    default:
      return "dynamic_tool_call";
  }
}

export function makeAntigravityAdapter(
  settings: AntigravitySettings,
  options?: AntigravityAdapterLiveOptions,
) {
  return Effect.gen(function* () {
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("antigravity");
    const path = yield* Path.Path;
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const crypto = yield* Crypto.Crypto;

    const sessions = new Map<ThreadId, AntigravitySessionContext>();
    const threadLocksRef = yield* SynchronizedRef.make(new Map<string, Semaphore.Semaphore>());
    const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const randomUUIDv4 = crypto.randomUUIDv4.pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "crypto/randomUUIDv4",
            detail: "Failed to generate Antigravity runtime identifier.",
            cause,
          }),
      ),
    );
    const nextEventId = Effect.map(randomUUIDv4, (id) => EventId.make(id));
    const makeEventStamp = () => Effect.all({ eventId: nextEventId, createdAt: nowIso });

    const offerRuntimeEvent = (event: ProviderRuntimeEvent) =>
      PubSub.publish(runtimeEventPubSub, event).pipe(Effect.asVoid);

    const getThreadSemaphore = (threadId: string) =>
      SynchronizedRef.modifyEffect(threadLocksRef, (current) => {
        const existing: Option.Option<Semaphore.Semaphore> = Option.fromNullishOr(
          current.get(threadId),
        );
        return Option.match(existing, {
          onNone: () =>
            Semaphore.make(1).pipe(
              Effect.map((semaphore) => {
                const next = new Map(current);
                next.set(threadId, semaphore);
                return [semaphore, next] as const;
              }),
            ),
          onSome: (semaphore) => Effect.succeed([semaphore, current] as const),
        });
      });

    const withThreadLock = <A, E, R>(threadId: string, effect: Effect.Effect<A, E, R>) =>
      Effect.flatMap(getThreadSemaphore(threadId), (semaphore) => semaphore.withPermit(effect));

    const requireSession = (
      threadId: ThreadId,
    ): Effect.Effect<AntigravitySessionContext, ProviderAdapterSessionNotFoundError> => {
      const ctx = sessions.get(threadId);
      if (!ctx || ctx.stopped) {
        return Effect.fail(
          new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }),
        );
      }
      return Effect.succeed(ctx);
    };

    const stopSessionInternal = (ctx: AntigravitySessionContext) =>
      Effect.gen(function* () {
        if (ctx.stopped) return;
        ctx.stopped = true;
        if (ctx.activeProcess) {
          yield* ctx.activeProcess.kill("SIGKILL").pipe(Effect.ignore);
          ctx.activeProcess = null;
        }
        yield* Effect.ignore(Scope.close(ctx.scope, Exit.void));
        sessions.delete(ctx.threadId);
        yield* offerRuntimeEvent({
          type: "session.exited",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          payload: { exitKind: "graceful" },
        });
      });

    const startSession: AntigravityAdapterShape["startSession"] = (input) =>
      withThreadLock(
        input.threadId,
        Effect.gen(function* () {
          if (input.provider !== undefined && input.provider !== PROVIDER) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
            });
          }
          if (!input.cwd?.trim()) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: "cwd is required and must be non-empty.",
            });
          }

          const cwd = path.resolve(input.cwd.trim());
          const existing = sessions.get(input.threadId);
          if (existing && !existing.stopped) {
            yield* stopSessionInternal(existing);
          }

          const sessionScope = yield* Scope.make("sequential");
          const resumeConversationId =
            parseAntigravityResume(input.resumeCursor)?.conversationId ?? null;
          const now = yield* nowIso;

          const session: ProviderSession = {
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            status: "ready",
            runtimeMode: input.runtimeMode,
            cwd,
            ...(input.modelSelection?.model ? { model: input.modelSelection.model } : {}),
            threadId: input.threadId,
            ...(resumeConversationId
              ? {
                  resumeCursor: {
                    schemaVersion: ANTIGRAVITY_RESUME_VERSION,
                    conversationId: resumeConversationId,
                  },
                }
              : {}),
            createdAt: now,
            updatedAt: now,
          };

          const ctx: AntigravitySessionContext = {
            threadId: input.threadId,
            conversationId: resumeConversationId,
            session,
            scope: sessionScope,
            activeProcess: null,
            activeTurnId: undefined,
            turns: [],
            knownSubagents: new Set(),
            stopped: false,
          };

          sessions.set(input.threadId, ctx);

          yield* offerRuntimeEvent({
            type: "session.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: {
              resume: resumeConversationId ? { conversationId: resumeConversationId } : null,
            },
          });

          yield* offerRuntimeEvent({
            type: "session.state.changed",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { state: "ready", reason: "Antigravity session ready" },
          });

          yield* offerRuntimeEvent({
            type: "thread.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { providerThreadId: resumeConversationId ?? input.threadId },
          });

          return session;
        }),
      );

    const sendTurn: AntigravityAdapterShape["sendTurn"] = (input) =>
      Effect.gen(function* () {
        const turnId = TurnId.make(yield* randomUUIDv4);
        const ctx = yield* requireSession(input.threadId);

        ctx.activeTurnId = turnId;
        ctx.session = {
          ...ctx.session,
          status: "running",
          activeTurnId: turnId,
          updatedAt: yield* nowIso,
        };

        const promptText = input.input ?? (input as any).prompt ?? "";
        const commandName = settings.binaryPath || "agy";
        const args = [
          "-p",
          promptText,
          "--output-format",
          "stream-json",
          "--dangerously-skip-permissions",
        ];

        if (ctx.session.cwd) {
          args.push("--add-dir", ctx.session.cwd);
        }
        if (ctx.conversationId) {
          args.push("--conversation", ctx.conversationId);
        }
        if (input.modelSelection?.model) {
          args.push("--model", input.modelSelection.model);
        }

        const spawnCommand = yield* resolveSpawnCommand(commandName, args, {
          env: options?.environment ?? process.env,
        });

        yield* offerRuntimeEvent({
          type: "turn.started",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: input.threadId,
          turnId,
          payload: {
            ...(input.modelSelection?.model ? { model: input.modelSelection.model } : {}),
          },
        });

        let completedEmitted = false;
        const stepItemIds = new Map<number | string, RuntimeItemId>();

        const handleNdjsonLine = (line: string) =>
          Effect.gen(function* () {
            let parsed: Record<string, unknown>;
            try {
              parsed = JSON.parse(line);
            } catch {
              return;
            }

            const eventType = parsed.event;

            if (eventType === "init") {
              const convId =
                typeof parsed.conversation_id === "string"
                  ? parsed.conversation_id
                  : isRecord(parsed.init) && typeof parsed.init.conversation_id === "string"
                    ? (parsed.init.conversation_id as string)
                    : undefined;
              if (convId) {
                ctx.conversationId = convId;
                ctx.session = {
                  ...ctx.session,
                  resumeCursor: {
                    schemaVersion: ANTIGRAVITY_RESUME_VERSION,
                    conversationId: convId,
                  },
                  updatedAt: yield* nowIso,
                };
              }
              return;
            }

            if (eventType === "step_update") {
              const update = isRecord(parsed.step_update) ? parsed.step_update : parsed;
              const stepType = typeof update.step_type === "string" ? update.step_type : undefined;
              const stepIndex = typeof update.step_index === "number" ? update.step_index : 0;
              const state = typeof update.state === "string" ? update.state : "DONE";

              if (stepType === "agent_response") {
                const textDelta = typeof update.text_delta === "string" ? update.text_delta : "";
                if (textDelta.length > 0) {
                  yield* offerRuntimeEvent({
                    type: "content.delta",
                    ...(yield* makeEventStamp()),
                    provider: PROVIDER,
                    threadId: input.threadId,
                    turnId,
                    payload: {
                      streamKind: "assistant_text",
                      delta: textDelta,
                    },
                  });
                }
                return;
              }

              if (stepType === "tool") {
                const toolInfo = isRecord(update.tool_info) ? update.tool_info : undefined;
                const toolName =
                  typeof update.tool_name === "string"
                    ? update.tool_name
                    : typeof toolInfo?.name === "string"
                      ? (toolInfo.name as string)
                      : "tool";
                const itemType = classifyToolName(toolName);
                const subagentInfo = isRecord(update.subagent_info)
                  ? update.subagent_info
                  : undefined;

                if (subagentInfo && typeof subagentInfo.conversation_id === "string") {
                  const subagentId = subagentInfo.conversation_id;
                  const role =
                    typeof subagentInfo.role === "string" ? subagentInfo.role : "Subagent";
                  const status =
                    typeof subagentInfo.status === "string" ? subagentInfo.status : "running";

                  if (!ctx.knownSubagents.has(subagentId)) {
                    ctx.knownSubagents.add(subagentId);
                    yield* offerRuntimeEvent({
                      type: "task.started",
                      ...(yield* makeEventStamp()),
                      provider: PROVIDER,
                      threadId: input.threadId,
                      turnId,
                      payload: {
                        taskType: "subagent",
                        agentId: subagentId,
                        parentAgentId: ctx.conversationId ?? undefined,
                        role,
                        title: role,
                        timelineBypass: true,
                      },
                    });
                  } else {
                    yield* offerRuntimeEvent({
                      type:
                        status === "completed" || status === "failed"
                          ? "task.completed"
                          : "task.updated",
                      ...(yield* makeEventStamp()),
                      provider: PROVIDER,
                      threadId: input.threadId,
                      turnId,
                      payload: {
                        agentId: subagentId,
                        title: role,
                        ...(status === "failed" ? { error: "Subagent task failed" } : {}),
                      },
                    });
                  }
                }

                if (toolInfo || typeof update.tool_name === "string") {
                  let itemId = stepItemIds.get(stepIndex);
                  const isNewItem = !itemId;
                  if (!itemId) {
                    const rawItemId = yield* randomUUIDv4;
                    itemId = RuntimeItemId.make(rawItemId);
                    stepItemIds.set(stepIndex, itemId);
                  }

                  if (state === "ACTIVE" || isNewItem) {
                    yield* offerRuntimeEvent({
                      type: "item.started",
                      ...(yield* makeEventStamp()),
                      provider: PROVIDER,
                      threadId: input.threadId,
                      turnId,
                      itemId,
                      payload: {
                        itemType,
                        title: toolName,
                        detail: JSON.stringify(toolInfo?.parameters ?? {}),
                      },
                    });
                  }

                  if (state === "DONE") {
                    const isError = toolInfo?.status === "error" || update.status === "error";
                    const status = isError ? "failed" : "completed";
                    const outputText =
                      typeof toolInfo?.output === "string" ? toolInfo.output : undefined;

                    yield* offerRuntimeEvent({
                      type: "item.completed",
                      ...(yield* makeEventStamp()),
                      provider: PROVIDER,
                      threadId: input.threadId,
                      turnId,
                      itemId,
                      payload: {
                        itemType,
                        status,
                        title: toolName,
                        detail: outputText?.slice(0, 2000),
                      },
                    });
                  }
                }
                return;
              }
            }

            if (eventType === "result") {
              completedEmitted = true;
              const res = isRecord(parsed.result) ? parsed.result : parsed;
              const rawStatus = String(res.status ?? "").toUpperCase();
              const status =
                rawStatus === "ERROR" || rawStatus === "FAILED" ? "failed" : "completed";
              const usage = isRecord(res.usage) ? res.usage : undefined;

              yield* offerRuntimeEvent({
                type: "turn.completed",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId: input.threadId,
                turnId,
                payload: {
                  state: status,
                  ...(usage
                    ? {
                        usage: {
                          inputTokens: Number(usage.input_tokens) || undefined,
                          outputTokens: Number(usage.output_tokens) || undefined,
                          totalTokens: Number(usage.total_tokens) || undefined,
                        },
                      }
                    : {}),
                },
              });

              ctx.session = {
                ...ctx.session,
                status: "ready",
                activeTurnId: undefined,
                updatedAt: yield* nowIso,
              };
              ctx.activeTurnId = undefined;
              ctx.activeProcess = null;
            }
          });

        const executeTurnProcess = Effect.gen(function* () {
          const command = ChildProcess.make(spawnCommand.command, spawnCommand.args, {
            env: options?.environment ?? process.env,
            cwd: ctx.session.cwd,
            shell: spawnCommand.shell,
          });

          const child = yield* childProcessSpawner.spawn(command);
          ctx.activeProcess = child;

          let buffer = "";
          const processStdout = child.stdout.pipe(
            Stream.runForEach((chunk) =>
              Effect.gen(function* () {
                buffer += Buffer.from(chunk).toString("utf8");
                const lines = buffer.split("\n");
                buffer = lines.pop() ?? "";
                for (const line of lines) {
                  const trimmed = line.trim();
                  if (trimmed.length > 0) {
                    yield* handleNdjsonLine(trimmed);
                  }
                }
              }),
            ),
          );

          const drainStderr = collectUint8StreamText({ stream: child.stderr }).pipe(Effect.ignore);

          yield* Effect.all([processStdout, drainStderr], { concurrency: "unbounded" });

          if (buffer.trim().length > 0) {
            yield* handleNdjsonLine(buffer.trim());
          }

          const exitCode = yield* child.exitCode.pipe(Effect.map(Number));
          if (!completedEmitted) {
            yield* offerRuntimeEvent({
              type: "turn.completed",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: input.threadId,
              turnId,
              payload: {
                state: exitCode === 0 ? "completed" : "failed",
                ...(exitCode !== 0
                  ? { errorMessage: `Antigravity CLI exited with code ${exitCode}` }
                  : {}),
              },
            });

            ctx.session = {
              ...ctx.session,
              status: "ready",
              activeTurnId: undefined,
              updatedAt: yield* nowIso,
            };
            ctx.activeTurnId = undefined;
            ctx.activeProcess = null;
          }
        }).pipe(
          Effect.scoped,
          Effect.catchCause((cause) =>
            Effect.gen(function* () {
              yield* Effect.logError("Antigravity turn processing failed", { cause });
              if (!completedEmitted) {
                yield* offerRuntimeEvent({
                  type: "turn.completed",
                  ...(yield* makeEventStamp()),
                  provider: PROVIDER,
                  threadId: input.threadId,
                  turnId,
                  payload: {
                    state: "failed",
                    errorMessage: "Antigravity process stream encountered an error.",
                  },
                });
                ctx.session = {
                  ...ctx.session,
                  status: "ready",
                  activeTurnId: undefined,
                  updatedAt: yield* nowIso,
                };
                ctx.activeTurnId = undefined;
                ctx.activeProcess = null;
              }
            }),
          ),
        );

        yield* executeTurnProcess;

        return {
          turnId,
          ...(ctx.session.resumeCursor !== undefined
            ? { resumeCursor: ctx.session.resumeCursor }
            : {}),
        } satisfies ProviderTurnStartResult;
      });

    const interruptTurn: AntigravityAdapterShape["interruptTurn"] = Effect.fn("interruptTurn")(
      function* (threadId, turnId) {
        const ctx = yield* requireSession(threadId);
        if (ctx.activeProcess) {
          yield* ctx.activeProcess.kill("SIGKILL").pipe(Effect.ignore);
          ctx.activeProcess = null;
        }

        const effectiveTurnId = turnId ?? ctx.activeTurnId;
        if (effectiveTurnId) {
          yield* offerRuntimeEvent({
            type: "turn.aborted",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId,
            turnId: effectiveTurnId,
            payload: { reason: "Interrupted by user." },
          });
        }

        ctx.session = {
          ...ctx.session,
          status: "ready",
          activeTurnId: undefined,
          updatedAt: yield* nowIso,
        };
        ctx.activeTurnId = undefined;
      },
    );

    const respondToRequest: AntigravityAdapterShape["respondToRequest"] = () => Effect.void;

    const respondToUserInput: AntigravityAdapterShape["respondToUserInput"] = () => Effect.void;

    const stopSession: AntigravityAdapterShape["stopSession"] = Effect.fn("stopSession")(
      function* (threadId) {
        const ctx = sessions.get(threadId);
        if (!ctx) {
          return yield* new ProviderAdapterSessionNotFoundError({
            provider: PROVIDER,
            threadId,
          });
        }
        yield* stopSessionInternal(ctx);
      },
    );

    const listSessions: AntigravityAdapterShape["listSessions"] = () =>
      Effect.sync(() => [...sessions.values()].map((ctx) => ctx.session));

    const hasSession: AntigravityAdapterShape["hasSession"] = (threadId) =>
      Effect.sync(() => sessions.has(threadId));

    const readThread: AntigravityAdapterShape["readThread"] = (threadId) =>
      Effect.sync(() => {
        const ctx = sessions.get(threadId);
        return {
          threadId,
          turns: ctx?.turns ?? [],
        };
      });

    const rollbackThread: AntigravityAdapterShape["rollbackThread"] = (threadId) =>
      Effect.sync(() => {
        const ctx = sessions.get(threadId);
        return {
          threadId,
          turns: ctx?.turns ?? [],
        };
      });

    const stopAll: AntigravityAdapterShape["stopAll"] = () =>
      Effect.forEach(Array.from(sessions.values()), (ctx) => stopSessionInternal(ctx), {
        discard: true,
      });

    return {
      provider: PROVIDER,
      capabilities: {
        sessionModelSwitch: "in-session",
      },
      startSession,
      sendTurn,
      interruptTurn,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      hasSession,
      readThread,
      rollbackThread,
      stopAll,
      streamEvents: Stream.fromPubSub(runtimeEventPubSub),
    } satisfies AntigravityAdapterShape;
  });
}
