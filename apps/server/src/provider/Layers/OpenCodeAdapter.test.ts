import * as NodeAssert from "node:assert/strict";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { beforeEach } from "vite-plus/test";

import {
  OpenCodeSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { ProviderSessionDirectory } from "../Services/ProviderSessionDirectory.ts";
import type { OpenCodeAdapterShape } from "../Services/OpenCodeAdapter.ts";
import {
  OpenCodeRuntime,
  OpenCodeRuntimeError,
  type OpenCodeRuntimeShape,
} from "../opencodeRuntime.ts";
import {
  appendOpenCodeAssistantTextDelta,
  isOpenCodeNotFound,
  isSameOpenCodeDirectory,
  makeOpenCodeAdapter,
  mergeOpenCodeAssistantText,
} from "./OpenCodeAdapter.ts";

// Test-local service tag so the rest of the file can keep using `yield* OpenCodeAdapter`.
class OpenCodeAdapter extends Context.Service<OpenCodeAdapter, OpenCodeAdapterShape>()(
  "t3/provider/Layers/OpenCodeAdapter.test/OpenCodeAdapter",
) {}

const asThreadId = (value: string): ThreadId => ThreadId.make(value);

type MessageEntry = {
  info: {
    id: string;
    role: "user" | "assistant";
    [key: string]: unknown;
  };
  parts: Array<unknown>;
};

const runtimeMock = {
  state: {
    startCalls: [] as string[],
    sessionCreateUrls: [] as string[],
    sessionCreateInputs: [] as Array<Record<string, unknown>>,
    authHeaders: [] as Array<string | null>,
    abortCalls: [] as string[],
    abortFailures: new Set<string>(),
    closeCalls: [] as string[],
    revertCalls: [] as Array<{ sessionID: string; messageID?: string }>,
    promptCalls: [] as Array<unknown>,
    promptAsyncError: null as Error | null,
    closeError: null as Error | null,
    messages: [] as MessageEntry[],
    childMessages: new Map<string, MessageEntry[]>(),
    messageCalls: [] as string[],
    subscribedEvents: [] as unknown[],
    sessionStatusCalls: [] as Array<{ directory?: string }>,
    sessionStatusMap: {} as Record<string, unknown>,
    sessionStatusMaps: [] as Array<Record<string, unknown>>,
    sessionGetIds: [] as string[],
    missingSessionIds: new Set<string>(),
    transientErrorSessionIds: new Set<string>(),
    sessionDirectoryById: new Map<string, string>(),
    sessionUpdateCalls: [] as Array<{ sessionID: string; permission: unknown }>,
    forkCalls: [] as Array<{ sessionID: string; directory?: string }>,
  },
  reset() {
    this.state.startCalls.length = 0;
    this.state.sessionCreateUrls.length = 0;
    this.state.sessionCreateInputs.length = 0;
    this.state.authHeaders.length = 0;
    this.state.abortCalls.length = 0;
    this.state.abortFailures.clear();
    this.state.closeCalls.length = 0;
    this.state.revertCalls.length = 0;
    this.state.promptCalls.length = 0;
    this.state.promptAsyncError = null;
    this.state.closeError = null;
    this.state.messages = [];
    this.state.childMessages.clear();
    this.state.messageCalls.length = 0;
    this.state.subscribedEvents = [];
    this.state.sessionStatusCalls.length = 0;
    this.state.sessionStatusMap = {};
    this.state.sessionStatusMaps.length = 0;
    this.state.sessionGetIds.length = 0;
    this.state.missingSessionIds.clear();
    this.state.transientErrorSessionIds.clear();
    this.state.sessionDirectoryById.clear();
    this.state.sessionUpdateCalls.length = 0;
    this.state.forkCalls.length = 0;
  },
};

const OpenCodeRuntimeTestDouble: OpenCodeRuntimeShape = {
  startOpenCodeServerProcess: ({ binaryPath }) =>
    Effect.gen(function* () {
      runtimeMock.state.startCalls.push(binaryPath);
      const url = "http://127.0.0.1:4301";
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          runtimeMock.state.closeCalls.push(url);
          if (runtimeMock.state.closeError) {
            throw runtimeMock.state.closeError;
          }
        }),
      );
      return {
        url,
        exitCode: Effect.never,
      };
    }),
  connectToOpenCodeServer: ({ serverUrl }) =>
    Effect.gen(function* () {
      const url = serverUrl ?? "http://127.0.0.1:4301";
      // Always register a finalizer so the closeCalls/closeError probes fire;
      // production attaches none for external servers.
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          runtimeMock.state.closeCalls.push(url);
          if (runtimeMock.state.closeError) {
            throw runtimeMock.state.closeError;
          }
        }),
      );
      return {
        url,
        exitCode: null,
        external: Boolean(serverUrl),
      };
    }),
  runOpenCodeCommand: () => Effect.succeed({ stdout: "", stderr: "", code: 0 }),
  createOpenCodeSdkClient: ({ baseUrl, serverPassword }) =>
    ({
      session: {
        create: async (input: Record<string, unknown>) => {
          runtimeMock.state.sessionCreateUrls.push(baseUrl);
          runtimeMock.state.sessionCreateInputs.push(input);
          runtimeMock.state.authHeaders.push(
            serverPassword ? `Basic ${btoa(`opencode:${serverPassword}`)}` : null,
          );
          return { data: { id: `${baseUrl}/session` } };
        },
        get: async ({ sessionID }: { sessionID: string }) => {
          runtimeMock.state.sessionGetIds.push(sessionID);
          // The real client is `throwOnError: true`: non-2xx rejects rather
          // than resolving, so missing → 404 throw, transient → 500 throw.
          if (runtimeMock.state.transientErrorSessionIds.has(sessionID)) {
            throw new Error("opencode server error", { cause: { status: 500 } });
          }
          if (runtimeMock.state.missingSessionIds.has(sessionID)) {
            throw new Error(`Session not found: ${sessionID}`, {
              cause: { status: 404, body: { name: "NotFoundError" } },
            });
          }
          const directory = runtimeMock.state.sessionDirectoryById.get(sessionID);
          return { data: { id: sessionID, ...(directory ? { directory } : {}) } };
        },
        update: async ({ sessionID, permission }: { sessionID: string; permission: unknown }) => {
          runtimeMock.state.sessionUpdateCalls.push({ sessionID, permission });
          return { data: { id: sessionID } };
        },
        fork: async ({ sessionID, directory }: { sessionID: string; directory?: string }) => {
          // Fork clones history into a new session bound to the directory.
          const forkedId = `${sessionID}_fork`;
          runtimeMock.state.forkCalls.push({ sessionID, ...(directory ? { directory } : {}) });
          if (directory) {
            runtimeMock.state.sessionDirectoryById.set(forkedId, directory);
          }
          return { data: { id: forkedId, ...(directory ? { directory } : {}) } };
        },
        abort: async ({ sessionID }: { sessionID: string }) => {
          runtimeMock.state.abortCalls.push(sessionID);
          if (runtimeMock.state.abortFailures.has(sessionID)) {
            throw new Error(`abort failed: ${sessionID}`);
          }
        },
        promptAsync: async (input: unknown) => {
          runtimeMock.state.promptCalls.push(input);
          if (runtimeMock.state.promptAsyncError) {
            throw runtimeMock.state.promptAsyncError;
          }
        },
        messages: async ({ sessionID }: { sessionID: string }) => {
          runtimeMock.state.messageCalls.push(sessionID);
          return {
            data: runtimeMock.state.childMessages.get(sessionID) ?? runtimeMock.state.messages,
          };
        },
        revert: async ({ sessionID, messageID }: { sessionID: string; messageID?: string }) => {
          runtimeMock.state.revertCalls.push({
            sessionID,
            ...(messageID ? { messageID } : {}),
          });
          if (!messageID) {
            runtimeMock.state.messages = [];
            return;
          }

          const targetIndex = runtimeMock.state.messages.findIndex(
            (entry) => entry.info.id === messageID,
          );
          runtimeMock.state.messages =
            targetIndex >= 0
              ? runtimeMock.state.messages.slice(0, targetIndex + 1)
              : runtimeMock.state.messages;
        },
        status: async ({ directory }: { directory?: string } = {}) => {
          runtimeMock.state.sessionStatusCalls.push({ ...(directory ? { directory } : {}) });
          const statusMap =
            runtimeMock.state.sessionStatusMaps[runtimeMock.state.sessionStatusCalls.length - 1] ??
            runtimeMock.state.sessionStatusMap;
          return { data: statusMap };
        },
      },
      event: {
        subscribe: async () => ({
          stream: (async function* () {
            for (const event of runtimeMock.state.subscribedEvents) {
              yield event;
            }
          })(),
        }),
      },
    }) as unknown as ReturnType<OpenCodeRuntimeShape["createOpenCodeSdkClient"]>,
  loadOpenCodeInventory: () =>
    Effect.fail(
      new OpenCodeRuntimeError({
        operation: "loadOpenCodeInventory",
        detail: "OpenCodeRuntimeTestDouble.loadOpenCodeInventory not used in this test",
        cause: null,
      }),
    ),
  loadInventoryFromCli: () =>
    Effect.fail(
      new OpenCodeRuntimeError({
        operation: "loadInventoryFromCli",
        detail: "OpenCodeRuntimeTestDouble.loadInventoryFromCli not used in this test",
        cause: null,
      }),
    ),
};

const providerSessionDirectoryTestLayer = Layer.succeed(ProviderSessionDirectory, {
  upsert: () => Effect.void,
  getProvider: () =>
    Effect.die(new Error("ProviderSessionDirectory.getProvider is not used in test")),
  getBinding: () => Effect.succeed(Option.none()),
  listThreadIds: () => Effect.succeed([]),
  listBindings: () => Effect.succeed([]),
});

// The adapter now receives its settings as a plain argument (the old design
// read from `ServerSettingsService` internally). The test-only
// `ServerSettingsService` below is still kept because other dependencies in
// the layer graph reach for it — but the routing values the assertions
// probe (serverUrl, serverPassword) must be threaded directly through the
// decoded `OpenCodeSettings`.
const openCodeAdapterTestSettings = Schema.decodeSync(OpenCodeSettings)({
  binaryPath: "fake-opencode",
  serverUrl: "http://127.0.0.1:9999",
  serverPassword: "secret-password",
});

const OpenCodeAdapterTestLayer = Layer.effect(
  OpenCodeAdapter,
  makeOpenCodeAdapter(openCodeAdapterTestSettings),
).pipe(
  Layer.provideMerge(Layer.succeed(OpenCodeRuntime, OpenCodeRuntimeTestDouble)),
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
  Layer.provideMerge(
    ServerSettingsService.layerTest({
      providers: {
        opencode: {
          binaryPath: "fake-opencode",
          serverUrl: "http://127.0.0.1:9999",
          serverPassword: "secret-password",
        },
      },
    }),
  ),
  Layer.provideMerge(providerSessionDirectoryTestLayer),
  Layer.provideMerge(NodeServices.layer),
);

beforeEach(() => {
  runtimeMock.reset();
});

const advanceTestClock = (ms: number) =>
  TestClock.adjust(`${ms} millis`).pipe(Effect.andThen(Effect.yieldNow));

const rootOpenCodeSessionId = "http://127.0.0.1:9999/session";

const makeTaskPart = (input: {
  readonly childId: string;
  readonly partId: string;
  readonly status: "running" | "completed" | "error";
  readonly description?: string;
  readonly role?: string;
  readonly background?: boolean;
  readonly parentSessionId?: string;
}) => {
  const description = input.description ?? "Review adapter behavior";
  const role = input.role ?? "reviewer";
  const metadata = {
    parentSessionId: input.parentSessionId ?? rootOpenCodeSessionId,
    sessionId: input.childId,
    model: { providerID: "anthropic", modelID: "claude-sonnet-4-5" },
    ...(input.background === true ? { background: true } : {}),
  };
  const taskInput = {
    description,
    prompt: "Inspect implementation",
    subagent_type: role,
    ...(input.background === true ? { background: true } : {}),
  };
  const state =
    input.status === "running"
      ? { status: "running", input: taskInput, title: description, time: { start: 1 }, metadata }
      : input.status === "completed"
        ? {
            status: "completed",
            input: taskInput,
            output: "Task result",
            title: description,
            time: { start: 1, end: 2 },
            metadata,
          }
        : {
            status: "error",
            input: taskInput,
            error: "Task failed",
            time: { start: 1, end: 2 },
            metadata,
          };
  return {
    id: input.partId,
    sessionID: rootOpenCodeSessionId,
    messageID: "root-assistant",
    type: "tool",
    callID: `${input.partId}-call`,
    tool: "task",
    state,
  };
};

const childSessionCreated = (childId: string, parentID = rootOpenCodeSessionId) => ({
  type: "session.created",
  properties: {
    sessionID: childId,
    info: { id: childId, parentID },
  },
});

const childSessionStatus = (
  childId: string,
  status: "busy" | "retry" | "idle",
  id = `${childId}-${status}`,
) => ({
  id,
  type: "session.status",
  properties: {
    sessionID: childId,
    status:
      status === "retry"
        ? { type: "retry", attempt: 1, message: "retrying", next: 1 }
        : { type: status },
  },
});

const isChildActivity = (
  event: { readonly type: string; readonly payload: unknown },
  childId: string,
): boolean => {
  if (
    event.type !== "task.started" &&
    event.type !== "task.progress" &&
    event.type !== "task.updated" &&
    event.type !== "task.completed" &&
    event.type !== "tool.progress"
  ) {
    return false;
  }
  const payload =
    typeof event.payload === "object" && event.payload !== null
      ? (event.payload as { readonly taskId?: unknown })
      : undefined;
  return String(payload?.taskId) === childId;
};

it.layer(OpenCodeAdapterTestLayer)("OpenCodeAdapterLive", (it) => {
  it.effect("reuses a configured OpenCode server URL instead of spawning a local server", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;

      const session = yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId: asThreadId("thread-opencode"),
        runtimeMode: "full-access",
      });

      NodeAssert.equal(session.provider, "opencode");
      NodeAssert.equal(session.threadId, "thread-opencode");
      NodeAssert.deepEqual(runtimeMock.state.startCalls, []);
      NodeAssert.deepEqual(runtimeMock.state.sessionCreateUrls, ["http://127.0.0.1:9999"]);
      NodeAssert.deepEqual(runtimeMock.state.authHeaders, [
        `Basic ${btoa("opencode:secret-password")}`,
      ]);
    }),
  );

  it.effect("returns a durable resume cursor for a freshly created session", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-opencode-cursor");

      const session = yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
      });

      // Without a persisted cursor, a session is created and its id is
      // surfaced as a resume cursor so the upper layer can persist it.
      NodeAssert.deepEqual(runtimeMock.state.sessionGetIds, []);
      NodeAssert.deepEqual(session.resumeCursor, {
        schemaVersion: 1,
        sessionId: "http://127.0.0.1:9999/session",
      });

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("resumes the persisted OpenCode session instead of creating a new one", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-opencode-resume");

      const session = yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
        resumeCursor: { schemaVersion: 1, sessionId: "ses_persisted" },
      });

      // The adapter validates the persisted id with session.get and re-adopts
      // it — no new session is minted (issue #3604).
      NodeAssert.deepEqual(runtimeMock.state.sessionGetIds, ["ses_persisted"]);
      NodeAssert.deepEqual(runtimeMock.state.sessionCreateUrls, []);
      NodeAssert.deepEqual(session.resumeCursor, {
        schemaVersion: 1,
        sessionId: "ses_persisted",
      });
      // Resume re-asserts the permission ruleset for the current runtimeMode.
      NodeAssert.equal(runtimeMock.state.sessionUpdateCalls.length, 1);
      NodeAssert.equal(runtimeMock.state.sessionUpdateCalls[0]?.sessionID, "ses_persisted");
      NodeAssert.equal(runtimeMock.state.sessionUpdateCalls[0]?.permission != null, true);

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("sends follow-up turns to the resumed session id", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-opencode-resume-turn");

      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
        resumeCursor: { schemaVersion: 1, sessionId: "ses_persisted" },
      });

      const result = yield* adapter.sendTurn({
        threadId,
        input: "continue where we left off",
        modelSelection: createModelSelection(
          ProviderInstanceId.make("opencode"),
          "anthropic/sonnet",
        ),
      });

      // The prompt targets the resumed id, and the turn re-surfaces the cursor.
      NodeAssert.deepEqual(
        (runtimeMock.state.promptCalls[0] as { sessionID: string }).sessionID,
        "ses_persisted",
      );
      NodeAssert.deepEqual(result.resumeCursor, {
        schemaVersion: 1,
        sessionId: "ses_persisted",
      });

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("falls back to a fresh session when the persisted session is gone", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-opencode-stale");
      runtimeMock.state.missingSessionIds.add("ses_stale");

      const session = yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
        resumeCursor: { schemaVersion: 1, sessionId: "ses_stale" },
      });

      // get probed the stale id, found nothing, then created a new session and
      // emitted a fresh cursor rather than wedging the thread.
      NodeAssert.deepEqual(runtimeMock.state.sessionGetIds, ["ses_stale"]);
      NodeAssert.deepEqual(runtimeMock.state.sessionCreateUrls, ["http://127.0.0.1:9999"]);
      NodeAssert.deepEqual(session.resumeCursor, {
        schemaVersion: 1,
        sessionId: "http://127.0.0.1:9999/session",
      });

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("ignores a malformed or wrong-version resume cursor", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-opencode-badcursor");

      const session = yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
        resumeCursor: { schemaVersion: 99, sessionId: "ses_persisted" },
      });

      // A foreign/stale-shaped cursor is treated as "no resume": never probed,
      // a fresh session is created.
      NodeAssert.deepEqual(runtimeMock.state.sessionGetIds, []);
      NodeAssert.deepEqual(runtimeMock.state.sessionCreateUrls, ["http://127.0.0.1:9999"]);
      NodeAssert.deepEqual(session.resumeCursor, {
        schemaVersion: 1,
        sessionId: "http://127.0.0.1:9999/session",
      });

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("surfaces a non-not-found resume probe error instead of silently starting fresh", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-opencode-transient");
      // session.get returns a 500 (not a 404) for this id.
      runtimeMock.state.transientErrorSessionIds.add("ses_transient");

      const exit = yield* Effect.exit(
        adapter.startSession({
          provider: ProviderDriverKind.make("opencode"),
          threadId,
          runtimeMode: "full-access",
          resumeCursor: { schemaVersion: 1, sessionId: "ses_transient" },
        }),
      );

      // A transient/transport/auth failure must propagate — NOT be masked as a
      // brand-new empty session (the #3604 class of silent context loss).
      NodeAssert.equal(Exit.isFailure(exit), true);
      NodeAssert.deepEqual(runtimeMock.state.sessionGetIds, ["ses_transient"]);
      NodeAssert.deepEqual(runtimeMock.state.sessionCreateUrls, []);
    }),
  );

  it.effect("re-applies the current runtimeMode permissions when resuming", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-opencode-perms");

      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        // A different runtimeMode than the original create — resume must not
        // leave the upstream session on stale permissions.
        runtimeMode: "approval-required",
        threadId,
        resumeCursor: { schemaVersion: 1, sessionId: "ses_perms" },
      });

      NodeAssert.deepEqual(runtimeMock.state.sessionGetIds, ["ses_perms"]);
      NodeAssert.deepEqual(runtimeMock.state.sessionCreateUrls, []);
      NodeAssert.equal(runtimeMock.state.sessionUpdateCalls.length, 1);
      NodeAssert.equal(runtimeMock.state.sessionUpdateCalls[0]?.sessionID, "ses_perms");
      NodeAssert.equal(runtimeMock.state.sessionUpdateCalls[0]?.permission != null, true);

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect(
    "forks the resumed session into the requested directory instead of losing context",
    () =>
      Effect.gen(function* () {
        const adapter = yield* OpenCodeAdapter;
        const threadId = asThreadId("thread-opencode-cwd");
        // The persisted session still exists but was created in another working dir
        // (e.g. the thread moved from the project root into a git worktree).
        runtimeMock.state.sessionDirectoryById.set("ses_otherdir", "/some/other/worktree");

        const session = yield* adapter.startSession({
          provider: ProviderDriverKind.make("opencode"),
          threadId,
          runtimeMode: "full-access",
          resumeCursor: { schemaVersion: 1, sessionId: "ses_otherdir" },
        });

        // A cwd change must not mint an empty session: the adapter forks the
        // persisted session into the requested cwd, carrying history forward.
        NodeAssert.deepEqual(runtimeMock.state.sessionGetIds, ["ses_otherdir"]);
        NodeAssert.deepEqual(runtimeMock.state.sessionCreateUrls, []);
        NodeAssert.equal(runtimeMock.state.forkCalls.length, 1);
        NodeAssert.equal(runtimeMock.state.forkCalls[0]?.sessionID, "ses_otherdir");
        NodeAssert.equal(typeof runtimeMock.state.forkCalls[0]?.directory, "string");
        // Permission ruleset re-asserted on the fork for the current runtimeMode.
        NodeAssert.equal(runtimeMock.state.sessionUpdateCalls.length, 1);
        NodeAssert.equal(runtimeMock.state.sessionUpdateCalls[0]?.sessionID, "ses_otherdir_fork");
        // Durable cursor now points at the history-complete fork in the new directory.
        NodeAssert.deepEqual(session.resumeCursor, {
          schemaVersion: 1,
          sessionId: "ses_otherdir_fork",
        });

        yield* adapter.stopSession(threadId);
      }),
  );

  it.effect("reuses the resumed session when the stored directory differs only lexically", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-opencode-samedir");
      // Same working tree, different spelling (trailing slash) — must reuse,
      // not fork.
      runtimeMock.state.sessionDirectoryById.set("ses_samedir", `${process.cwd()}/`);

      const session = yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
        resumeCursor: { schemaVersion: 1, sessionId: "ses_samedir" },
      });

      NodeAssert.deepEqual(runtimeMock.state.sessionGetIds, ["ses_samedir"]);
      NodeAssert.deepEqual(runtimeMock.state.sessionCreateUrls, []);
      NodeAssert.deepEqual(runtimeMock.state.forkCalls, []);
      NodeAssert.deepEqual(session.resumeCursor, {
        schemaVersion: 1,
        sessionId: "ses_samedir",
      });

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("fails sendTurn for missing sessions through the typed error channel", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const result = yield* adapter
        .sendTurn({
          threadId: asThreadId("thread-opencode-missing-send"),
          input: "hello",
          attachments: [],
        })
        .pipe(Effect.result);

      NodeAssert.equal(result._tag, "Failure");
      NodeAssert.equal(result.failure._tag, "ProviderAdapterSessionNotFoundError");
      NodeAssert.equal(result.failure.provider, "opencode");
      NodeAssert.equal(result.failure.threadId, "thread-opencode-missing-send");
    }),
  );

  it.effect("fails stopSession for missing sessions through the typed error channel", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const result = yield* adapter
        .stopSession(asThreadId("thread-opencode-missing-stop"))
        .pipe(Effect.result);

      NodeAssert.equal(result._tag, "Failure");
      NodeAssert.equal(result.failure._tag, "ProviderAdapterSessionNotFoundError");
      NodeAssert.equal(result.failure.provider, "opencode");
      NodeAssert.equal(result.failure.threadId, "thread-opencode-missing-stop");
    }),
  );

  it.effect("stops a configured-server session without trying to own server lifecycle", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId: asThreadId("thread-opencode"),
        runtimeMode: "full-access",
      });

      yield* adapter.stopSession(asThreadId("thread-opencode"));

      NodeAssert.deepEqual(runtimeMock.state.startCalls, []);
      NodeAssert.deepEqual(
        runtimeMock.state.abortCalls.includes("http://127.0.0.1:9999/session"),
        true,
      );
    }),
  );

  it.effect("emits one session.exited event when stopping a session", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-opencode-stop-event");
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.take(3),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
      });
      yield* adapter.stopSession(threadId);

      const events = Array.from(yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second")));
      NodeAssert.deepEqual(
        events.map((event) => event.type),
        ["session.started", "thread.started", "session.exited"],
      );
    }),
  );

  it.effect("clears session state even when cleanup finalizers throw", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId: asThreadId("thread-stop-all-a"),
        runtimeMode: "full-access",
      });
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId: asThreadId("thread-stop-all-b"),
        runtimeMode: "full-access",
      });

      runtimeMock.state.closeError = new Error("close failed");
      // `stopAll` relies on `stopOpenCodeContext`, which is typed as
      // never-failing. A throwing finalizer surfaces as a defect — `Effect.exit`
      // captures it so the assertions can still run. The key invariant we're
      // validating is "the sessions map and close-call probes reflect cleanup
      // attempts regardless of finalizer outcome".
      yield* Effect.exit(adapter.stopAll());
      const sessions = yield* adapter.listSessions();

      NodeAssert.deepEqual(runtimeMock.state.closeCalls, [
        "http://127.0.0.1:9999",
        "http://127.0.0.1:9999",
      ]);
      NodeAssert.deepEqual(sessions, []);
    }),
  );

  it.effect("completes streamEvents when the adapter scope closes", () =>
    Effect.gen(function* () {
      const scope = yield* Scope.make("sequential");
      let scopeClosed = false;

      try {
        const adapterLayer = Layer.effect(
          OpenCodeAdapter,
          makeOpenCodeAdapter(openCodeAdapterTestSettings),
        ).pipe(
          Layer.provideMerge(Layer.succeed(OpenCodeRuntime, OpenCodeRuntimeTestDouble)),
          Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
          Layer.provideMerge(ServerSettingsService.layerTest()),
          Layer.provideMerge(providerSessionDirectoryTestLayer),
          Layer.provideMerge(NodeServices.layer),
        );
        const context = yield* Layer.buildWithScope(adapterLayer, scope);
        const adapter = yield* Effect.service(OpenCodeAdapter).pipe(Effect.provide(context));
        const eventsFiber = yield* adapter.streamEvents.pipe(Stream.runCollect, Effect.forkChild);

        yield* Scope.close(scope, Exit.void);
        scopeClosed = true;

        const exit = yield* Fiber.await(eventsFiber).pipe(Effect.timeout("1 second"));
        NodeAssert.equal(Exit.hasInterrupts(exit), true);
      } finally {
        if (!scopeClosed) {
          yield* Scope.close(scope, Exit.void).pipe(Effect.ignore);
        }
      }
    }),
  );

  it.effect("rolls back session state when sendTurn fails before OpenCode accepts the prompt", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId: asThreadId("thread-send-turn-failure"),
        runtimeMode: "full-access",
      });

      runtimeMock.state.promptAsyncError = new Error("prompt failed");
      const error = yield* adapter
        .sendTurn({
          threadId: asThreadId("thread-send-turn-failure"),
          input: "Fix it",
          modelSelection: {
            instanceId: ProviderInstanceId.make("opencode"),
            model: "openai/gpt-5",
          },
        })
        .pipe(Effect.flip);
      const sessions = yield* adapter.listSessions();

      NodeAssert.equal(error._tag, "ProviderAdapterRequestError");
      if (error._tag !== "ProviderAdapterRequestError") {
        throw new Error("Unexpected error type");
      }
      NodeAssert.equal(error.detail, "prompt failed");
      NodeAssert.equal(
        error.message,
        "Provider adapter request failed (opencode) for session.promptAsync: prompt failed",
      );
      NodeAssert.equal(sessions.length, 1);
      NodeAssert.equal(sessions[0]?.status, "ready");
      NodeAssert.equal(sessions[0]?.activeTurnId, undefined);
      NodeAssert.equal(sessions[0]?.lastError, "prompt failed");
    }),
  );

  it.effect("steers a running turn instead of opening a new one on mid-turn sendTurn", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-steer");
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
      });

      const turn = yield* adapter.sendTurn({
        threadId,
        input: "run 5 commands",
        modelSelection: {
          instanceId: ProviderInstanceId.make("opencode"),
          model: "openai/gpt-5",
        },
      });

      // Steer: OpenCode queues the prompt into the busy session, so the
      // active turn id is reused instead of opening a new turn.
      const steeredTurn = yield* adapter.sendTurn({
        threadId,
        input: "actually run 15",
        modelSelection: {
          instanceId: ProviderInstanceId.make("opencode"),
          model: "openai/gpt-5",
        },
      });
      NodeAssert.equal(String(steeredTurn.turnId), String(turn.turnId));

      const sessions = yield* adapter.listSessions();
      const session = sessions.find((entry) => entry.threadId === threadId);
      NodeAssert.equal(session?.status, "running");
      NodeAssert.equal(String(session?.activeTurnId), String(turn.turnId));
      NodeAssert.equal(runtimeMock.state.promptCalls.length, 2);
    }),
  );

  it.effect("keeps the running turn when a steer prompt fails", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-steer-failure");
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
      });

      const turn = yield* adapter.sendTurn({
        threadId,
        input: "run 5 commands",
        modelSelection: {
          instanceId: ProviderInstanceId.make("opencode"),
          model: "openai/gpt-5",
        },
      });

      runtimeMock.state.promptAsyncError = new Error("steer failed");
      const error = yield* adapter
        .sendTurn({
          threadId,
          input: "actually run 15",
          modelSelection: {
            instanceId: ProviderInstanceId.make("opencode"),
            model: "openai/gpt-5",
          },
        })
        .pipe(Effect.flip);

      // The original turn keeps running — only the steer prompt failed.
      NodeAssert.equal(error._tag, "ProviderAdapterRequestError");
      const sessions = yield* adapter.listSessions();
      const session = sessions.find((entry) => entry.threadId === threadId);
      NodeAssert.equal(session?.status, "running");
      NodeAssert.equal(String(session?.activeTurnId), String(turn.turnId));
    }),
  );

  it.effect("passes agent and variant options for the adapter's bound custom instance id", () => {
    const instanceId = ProviderInstanceId.make("opencode_zen");
    const adapterLayer = Layer.effect(
      OpenCodeAdapter,
      makeOpenCodeAdapter(openCodeAdapterTestSettings, { instanceId }),
    ).pipe(
      Layer.provideMerge(Layer.succeed(OpenCodeRuntime, OpenCodeRuntimeTestDouble)),
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(ServerSettingsService.layerTest()),
      Layer.provideMerge(providerSessionDirectoryTestLayer),
      Layer.provideMerge(NodeServices.layer),
    );

    return Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId: asThreadId("thread-custom-instance"),
        runtimeMode: "full-access",
      });

      yield* adapter.sendTurn({
        threadId: asThreadId("thread-custom-instance"),
        input: "Fix it",
        modelSelection: createModelSelection(
          ProviderInstanceId.make("opencode_zen"),
          "anthropic/claude-sonnet-4-5",
          [
            { id: "agent", value: "github-copilot" },
            { id: "variant", value: "high" },
          ],
        ),
      });

      NodeAssert.deepEqual(runtimeMock.state.promptCalls.at(-1), {
        sessionID: "http://127.0.0.1:9999/session",
        model: {
          providerID: "anthropic",
          modelID: "claude-sonnet-4-5",
        },
        agent: "github-copilot",
        variant: "high",
        parts: [{ type: "text", text: "Fix it" }],
      });
    }).pipe(Effect.provide(adapterLayer));
  });

  it.effect("uses the bound custom instance id for fallback sendTurn model selection", () => {
    const instanceId = ProviderInstanceId.make("opencode_zen");
    const adapterLayer = Layer.effect(
      OpenCodeAdapter,
      makeOpenCodeAdapter(openCodeAdapterTestSettings, { instanceId }),
    ).pipe(
      Layer.provideMerge(Layer.succeed(OpenCodeRuntime, OpenCodeRuntimeTestDouble)),
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(ServerSettingsService.layerTest()),
      Layer.provideMerge(providerSessionDirectoryTestLayer),
      Layer.provideMerge(NodeServices.layer),
    );

    return Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-custom-instance-fallback-model");
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
        modelSelection: createModelSelection(
          ProviderInstanceId.make("opencode_zen"),
          "anthropic/claude-sonnet-4-5",
        ),
      });

      yield* adapter.sendTurn({
        threadId,
        input: "Fix it",
      });

      NodeAssert.deepEqual(runtimeMock.state.promptCalls.at(-1), {
        sessionID: "http://127.0.0.1:9999/session",
        model: {
          providerID: "anthropic",
          modelID: "claude-sonnet-4-5",
        },
        parts: [{ type: "text", text: "Fix it" }],
      });
    }).pipe(Effect.provide(adapterLayer));
  });

  it.effect("rejects sendTurn model selections for another instance id", () => {
    const instanceId = ProviderInstanceId.make("opencode_zen");
    const adapterLayer = Layer.effect(
      OpenCodeAdapter,
      makeOpenCodeAdapter(openCodeAdapterTestSettings, { instanceId }),
    ).pipe(
      Layer.provideMerge(Layer.succeed(OpenCodeRuntime, OpenCodeRuntimeTestDouble)),
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(ServerSettingsService.layerTest()),
      Layer.provideMerge(providerSessionDirectoryTestLayer),
      Layer.provideMerge(NodeServices.layer),
    );

    return Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-custom-instance-wrong-selection");
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
      });

      const error = yield* adapter
        .sendTurn({
          threadId,
          input: "Fix it",
          modelSelection: createModelSelection(
            ProviderInstanceId.make("opencode"),
            "anthropic/claude-sonnet-4-5",
          ),
        })
        .pipe(Effect.flip);

      NodeAssert.equal(error._tag, "ProviderAdapterValidationError");
      if (error._tag !== "ProviderAdapterValidationError") {
        throw new Error("Unexpected error type");
      }
      NodeAssert.equal(
        error.issue,
        "OpenCode model selection is bound to instance 'opencode', expected 'opencode_zen'.",
      );
      NodeAssert.deepEqual(runtimeMock.state.promptCalls, []);
    }).pipe(Effect.provide(adapterLayer));
  });

  it.effect("reverts the full thread when rollback removes every assistant turn", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-rollback-all");
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
      });

      runtimeMock.state.messages = [
        {
          info: { id: "assistant-1", role: "assistant" },
          parts: [],
        },
        {
          info: { id: "assistant-2", role: "assistant" },
          parts: [],
        },
      ];

      const snapshot = yield* adapter.rollbackThread(threadId, 2);

      NodeAssert.deepEqual(runtimeMock.state.revertCalls, [
        { sessionID: "http://127.0.0.1:9999/session" },
      ]);
      NodeAssert.deepEqual(snapshot.turns, []);
    }),
  );

  it.effect("classifies a confirmed not-found across the shapes the SDK/runtime can produce", () =>
    Effect.sync(() => {
      // The real production shape: runOpenCodeSdk wraps the thrown Error
      // (cause = { body, status }) under OpenCodeRuntimeError.
      const wrappedError = new Error("Session not found: ses_x", {
        cause: { body: { name: "NotFoundError" }, status: 404 },
      });
      NodeAssert.equal(
        isOpenCodeNotFound({
          _tag: "OpenCodeRuntimeError",
          operation: "session.get",
          detail: "Session not found: ses_x",
          cause: wrappedError,
        }),
        true,
      );

      // 404 expressed only via response.status (the bot's flagged shape).
      NodeAssert.equal(isOpenCodeNotFound({ cause: { response: { status: 404 } } }), true);
      // 404 via a bare numeric status / statusCode.
      NodeAssert.equal(isOpenCodeNotFound(new Error("x", { cause: { status: 404 } })), true);
      NodeAssert.equal(isOpenCodeNotFound({ statusCode: 404 }), true);
      // OpenCode NotFoundError body name with no status.
      NodeAssert.equal(isOpenCodeNotFound({ body: { name: "NotFoundError" } }), true);

      // NOT a miss: only structured signals count, never free text. A non-404
      // error whose message/detail merely contains "not found" must propagate,
      // not be misread as a missing session and silently start fresh.
      NodeAssert.equal(
        isOpenCodeNotFound(new Error("upstream provider not found", { cause: { status: 500 } })),
        false,
      );
      NodeAssert.equal(isOpenCodeNotFound({ detail: "status=500 body={...not found...}" }), false);
      // An explicit non-404 status seals its subtree: a 500 whose serialized
      // body echoes a NotFoundError name — or that is itself named
      // *NotFound* — is a real failure, never a miss.
      NodeAssert.equal(isOpenCodeNotFound({ status: 500, body: { name: "NotFoundError" } }), false);
      NodeAssert.equal(isOpenCodeNotFound({ name: "UpstreamNotFoundError", status: 500 }), false);
      // A "NotFound"-flavored name that isn't OpenCode's exact `NotFoundError`
      // is not a confirmed miss even without a sealing status.
      NodeAssert.equal(isOpenCodeNotFound({ name: "UpstreamNotFoundError" }), false);
      NodeAssert.equal(isOpenCodeNotFound({ cause: { name: "ProviderNotFoundError" } }), false);
      NodeAssert.equal(
        isOpenCodeNotFound(
          new Error("x", { cause: { status: 502, body: { name: "NotFoundError" } } }),
        ),
        false,
      );
      // Other transient/auth/network failures must propagate too.
      NodeAssert.equal(isOpenCodeNotFound(new Error("boom", { cause: { status: 500 } })), false);
      NodeAssert.equal(isOpenCodeNotFound({ cause: { response: { status: 401 } } }), false);
      NodeAssert.equal(isOpenCodeNotFound(new Error("network error (no response)")), false);
      NodeAssert.equal(isOpenCodeNotFound(undefined), false);
    }),
  );

  it.effect("treats lexically or physically identical directories as the same", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const sameDirectory = (left: string, right: string) =>
        isSameOpenCodeDirectory(fileSystem, path, left, right);

      // Lexical-only differences (trailing slash, dot segments) short-circuit
      // without touching the filesystem — the paths need not exist.
      NodeAssert.equal(yield* sameDirectory("/repo/project/", "/repo/project"), true);
      NodeAssert.equal(yield* sameDirectory("/repo/nested/../project", "/repo/project"), true);
      // Nonexistent paths degrade to the lexical comparison instead of failing.
      NodeAssert.equal(yield* sameDirectory("/repo/project", "/repo/other"), false);

      // A symlinked cwd (the macOS `/tmp` → `/private/tmp` shape) resolves to
      // the directory it points at, so the two spellings compare equal.
      const base = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-opencode-dir-" });
      const real = path.join(base, "real");
      const link = path.join(base, "link");
      yield* fileSystem.makeDirectory(real);
      yield* fileSystem.symlink(real, link);
      NodeAssert.equal(yield* sameDirectory(link, real), true);
      NodeAssert.equal(yield* sameDirectory(link, path.join(base, "other")), false);
    }).pipe(Effect.scoped),
  );

  it.effect("appends raw assistant text deltas and reconciles part update snapshots", () =>
    Effect.sync(() => {
      const firstUpdate = mergeOpenCodeAssistantText(undefined, "Hello");
      const overlapDelta = appendOpenCodeAssistantTextDelta(firstUpdate.latestText, "lo world");
      const secondUpdate = mergeOpenCodeAssistantText(overlapDelta.nextText, "Hellolo world");

      NodeAssert.deepEqual(
        [firstUpdate.deltaToEmit, overlapDelta.deltaToEmit, secondUpdate.deltaToEmit],
        ["Hello", "lo world", ""],
      );
      NodeAssert.equal(secondUpdate.latestText, "Hellolo world");
    }),
  );

  it.effect("does not strip coincidental prefix overlap from OpenCode part deltas", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-opencode-raw-delta");
      const part = {
        id: "part-raw-delta",
        sessionID: "http://127.0.0.1:9999/session",
        messageID: "msg-raw-delta",
        type: "text",
        text: "A B",
        time: { start: 1 },
      };
      runtimeMock.state.subscribedEvents = [
        {
          type: "message.updated",
          properties: {
            sessionID: "http://127.0.0.1:9999/session",
            info: {
              id: "msg-raw-delta",
              role: "assistant",
            },
          },
        },
        {
          type: "message.part.updated",
          properties: {
            sessionID: "http://127.0.0.1:9999/session",
            part,
            time: 1,
          },
        },
        {
          type: "message.part.delta",
          properties: {
            sessionID: "http://127.0.0.1:9999/session",
            messageID: "msg-raw-delta",
            partID: "part-raw-delta",
            field: "text",
            delta: "Bonus",
          },
        },
        {
          type: "message.part.updated",
          properties: {
            sessionID: "http://127.0.0.1:9999/session",
            part: {
              ...part,
              text: "A BBonus",
              time: { start: 1, end: 2 },
            },
            time: 2,
          },
        },
      ];
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.take(5),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
      });

      const events = Array.from(yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second")));
      const deltas = events.filter((event) => event.type === "content.delta");
      NodeAssert.deepEqual(
        deltas.map((event) => (event.type === "content.delta" ? event.payload.delta : "")),
        ["A B", "Bonus"],
      );
      NodeAssert.equal(events.at(-1)?.type, "item.completed");
      const completed = events.at(-1);
      if (completed?.type === "item.completed") {
        NodeAssert.equal(completed.payload.detail, "A BBonus");
      }
    }),
  );

  it.effect("keeps direct child traffic on task lifecycle without leaking child timeline rows", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-opencode-child-dispatch");
      const childId = "child-safe-dispatch";
      const childToolPart = {
        id: "child-tool-part",
        sessionID: childId,
        messageID: "child-message",
        type: "tool",
        callID: "child-tool-call",
        tool: "bash",
        state: {
          status: "running",
          input: { command: "printf secret" },
          title: "Running child command",
          time: { start: 1 },
        },
      };
      const completedChildToolPart = {
        ...childToolPart,
        state: {
          ...childToolPart.state,
          status: "completed",
          output: "secret",
          time: { start: 1, end: 2 },
        },
      };
      runtimeMock.state.sessionStatusMap = { [childId]: { type: "busy" } };
      runtimeMock.state.subscribedEvents = [
        {
          type: "message.part.updated",
          properties: {
            sessionID: rootOpenCodeSessionId,
            part: makeTaskPart({
              childId,
              partId: "root-task-part",
              status: "running",
            }),
            time: 1,
          },
        },
        {
          type: "message.updated",
          properties: {
            sessionID: childId,
            info: { id: "child-message", role: "assistant" },
          },
        },
        {
          type: "message.part.updated",
          properties: {
            sessionID: childId,
            part: {
              id: "child-text-part",
              sessionID: childId,
              messageID: "child-message",
              type: "text",
              text: "child assistant text must stay hidden",
            },
            time: 2,
          },
        },
        {
          type: "message.part.updated",
          properties: {
            sessionID: childId,
            part: childToolPart,
            time: 3,
          },
        },
        {
          type: "message.part.updated",
          properties: {
            sessionID: childId,
            part: completedChildToolPart,
            time: 4,
          },
        },
        {
          id: "child-error",
          type: "session.error",
          properties: {
            sessionID: childId,
            error: { data: { message: "child failed" } },
          },
        },
      ];

      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => isChildActivity(event, childId)),
        Stream.take(4),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
      });

      const events = Array.from(yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second")));
      const childEvents = events.filter((event) => isChildActivity(event, childId));
      NodeAssert.deepEqual(
        childEvents.map((event) => event.type),
        ["task.started", "tool.progress", "tool.progress", "task.completed"],
      );
      NodeAssert.equal(events.some((event) => event.type === "content.delta"), false);
      NodeAssert.equal(events.some((event) => event.type === "runtime.error"), false);
      NodeAssert.equal(events.some((event) => event.type === "turn.completed"), false);
      NodeAssert.equal(events.some((event) => event.type === "thread.metadata.updated"), false);
      NodeAssert.equal(
        events.filter(
          (event) => event.type.startsWith("item.") && isChildActivity(event, childId),
        ).length,
        0,
      );
    }),
  );

  it.effect("maps one status map to busy, retry, and missing-child idle states", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-opencode-child-status-map");
      const children = ["child-status-busy", "child-status-retry", "child-status-missing"];
      runtimeMock.state.sessionStatusMap = {
        [children[0]!]: { type: "busy" },
        [children[1]!]: { type: "retry", attempt: 1, message: "retrying", next: 1 },
      };
      runtimeMock.state.subscribedEvents = children.map((childId, index) => ({
        type: "message.part.updated",
        properties: {
          sessionID: rootOpenCodeSessionId,
          part: makeTaskPart({ childId, partId: `status-part-${index}`, status: "running" }),
          time: index + 1,
        },
      }));

      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => children.some((childId) => isChildActivity(event, childId))),
        Stream.take(5),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
      });

      const events = Array.from(yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second")));
      const statusEvents = events.filter((event) => event.type === "task.updated");
      NodeAssert.deepEqual(
        statusEvents.map((event) => (event.payload as { status?: string }).status),
        ["waiting", "idle"],
      );
      NodeAssert.equal(
        events.filter((event) => event.type === "task.started").length,
        3,
      );
      NodeAssert.equal(runtimeMock.state.sessionStatusCalls.length, 2);
      NodeAssert.deepEqual(runtimeMock.state.messageCalls, children);
    }),
  );

  it.effect("refreshes status coverage for children linked after hydration", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-opencode-child-status-refresh");
      const firstChild = "child-status-first";
      const lateChild = "child-status-late";
      runtimeMock.state.sessionStatusMaps = [
        { [firstChild]: { type: "retry", attempt: 1, message: "retrying", next: 1 } },
        { [firstChild]: { type: "retry", attempt: 1, message: "retrying", next: 1 }, [lateChild]: { type: "busy" } },
      ];
      runtimeMock.state.childMessages.set(lateChild, [
        {
          info: {
            id: "late-previous-assistant",
            role: "assistant",
            time: { created: 1, completed: 2 },
          },
          parts: [],
        },
      ]);
      runtimeMock.state.subscribedEvents = [
        {
          type: "message.part.updated",
          properties: {
            sessionID: rootOpenCodeSessionId,
            part: makeTaskPart({ childId: firstChild, partId: "status-first-part", status: "running" }),
            time: 1,
          },
        },
        {
          type: "message.part.updated",
          properties: {
            sessionID: rootOpenCodeSessionId,
            part: makeTaskPart({ childId: lateChild, partId: "status-late-part", status: "running" }),
            time: 2,
          },
        },
        childSessionStatus(lateChild, "retry", "status-late-retry"),
      ];

      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter(
          (event) => isChildActivity(event, firstChild) || isChildActivity(event, lateChild),
        ),
        Stream.take(4),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
      });

      const events = Array.from(yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second")));
      NodeAssert.deepEqual(
        events
          .filter((event) => event.type === "task.updated")
          .map((event) => (event.payload as { status?: string }).status),
        ["waiting", "waiting"],
      );
      NodeAssert.equal(events.some((event) => event.type === "task.completed"), false);
      NodeAssert.equal(runtimeMock.state.sessionStatusCalls.length, 2);
    }),
  );

  it.effect("keeps first-linked children resumable when history is already complete", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-opencode-child-first-link-resume");
      const childId = "child-first-link-resume";
      runtimeMock.state.childMessages.set(childId, [
        {
          info: {
            id: "first-link-previous-assistant",
            role: "assistant",
            time: { created: 1, completed: 2 },
          },
          parts: [],
        },
      ]);
      runtimeMock.state.sessionStatusMap = { [childId]: { type: "idle" } };
      runtimeMock.state.subscribedEvents = [
        {
          type: "message.part.updated",
          properties: {
            sessionID: rootOpenCodeSessionId,
            part: makeTaskPart({ childId, partId: "first-link-part", status: "running" }),
            time: 1,
          },
        },
        childSessionStatus(childId, "busy", "first-link-busy"),
      ];

      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => isChildActivity(event, childId)),
        Stream.take(3),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
      });

      const events = Array.from(yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second")));
      NodeAssert.deepEqual(events.map((event) => event.type), [
        "task.started",
        "task.updated",
        "task.updated",
      ]);
      NodeAssert.deepEqual(
        events
          .filter((event) => event.type === "task.updated")
          .map((event) => (event.payload as { status?: string }).status),
        ["idle", "running"],
      );
      NodeAssert.equal(events.some((event) => event.type === "task.completed"), false);
    }),
  );

  it.effect("applies current task failure before hydration state", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-opencode-child-current-failure");
      const childId = "child-current-failure";
      runtimeMock.state.childMessages.set(childId, [
        {
          info: {
            id: "current-failure-previous-assistant",
            role: "assistant",
            time: { created: 1, completed: 2 },
          },
          parts: [],
        },
      ]);
      runtimeMock.state.sessionStatusMap = { [childId]: { type: "idle" } };
      runtimeMock.state.subscribedEvents = [
        {
          type: "message.part.updated",
          properties: {
            sessionID: rootOpenCodeSessionId,
            part: makeTaskPart({ childId, partId: "current-failure-part", status: "error" }),
            time: 1,
          },
        },
      ];

      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => isChildActivity(event, childId)),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
      });

      const events = Array.from(yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second")));
      NodeAssert.deepEqual(events.map((event) => event.type), ["task.started", "task.completed"]);
      NodeAssert.equal(
        events[1]?.type === "task.completed" &&
          (events[1].payload as { status?: string }).status === "failed",
        true,
      );
    }),
  );

  it.effect("keeps background completion idle while foreground completion is terminal", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-opencode-child-background");
      const foreground = "child-foreground";
      const background = "child-background";
      runtimeMock.state.sessionStatusMap = { [foreground]: { type: "busy" } };
      runtimeMock.state.subscribedEvents = [
        {
          type: "message.part.updated",
          properties: {
            sessionID: rootOpenCodeSessionId,
            part: makeTaskPart({ childId: foreground, partId: "foreground-part", status: "running" }),
            time: 1,
          },
        },
        {
          type: "message.part.updated",
          properties: {
            sessionID: rootOpenCodeSessionId,
            part: makeTaskPart({
              childId: background,
              partId: "background-part",
              status: "running",
              background: true,
            }),
            time: 2,
          },
        },
        {
          type: "message.part.updated",
          properties: {
            sessionID: rootOpenCodeSessionId,
            part: makeTaskPart({ childId: foreground, partId: "foreground-part", status: "completed" }),
            time: 3,
          },
        },
        {
          type: "message.part.updated",
          properties: {
            sessionID: rootOpenCodeSessionId,
            part: makeTaskPart({
              childId: background,
              partId: "background-part",
              status: "completed",
              background: true,
            }),
            time: 4,
          },
        },
      ];

      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => isChildActivity(event, foreground) || isChildActivity(event, background)),
        Stream.take(4),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
      });

      const events = Array.from(yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second")));
      const completed = events.filter((event) => event.type === "task.completed");
      NodeAssert.equal(completed.length, 1);
      NodeAssert.equal(
        completed[0]?.type === "task.completed" && String(completed[0].payload.taskId) === foreground,
        true,
      );
      NodeAssert.equal(
        events.some(
          (event) =>
            event.type === "task.updated" &&
            String((event.payload as { taskId?: unknown }).taskId) === background &&
            (event.payload as { status?: string }).status === "idle",
        ),
        true,
      );
      NodeAssert.equal(
        events.some(
          (event) =>
            event.type === "task.completed" &&
            String((event.payload as { taskId?: unknown }).taskId) === background,
        ),
        false,
      );
    }),
  );

  it.effect("reactivates one child id with an explicit running update", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-opencode-child-resume");
      const childId = "child-resume";
      runtimeMock.state.sessionStatusMap = { [childId]: { type: "busy" } };
      runtimeMock.state.childMessages.set(childId, [
        {
          info: {
            id: "previous-assistant",
            role: "assistant",
            time: { created: 1, completed: 2 },
          },
          parts: [],
        },
      ]);
      runtimeMock.state.subscribedEvents = [
        {
          type: "message.part.updated",
          properties: {
            sessionID: rootOpenCodeSessionId,
            part: makeTaskPart({ childId, partId: "resume-part-1", status: "running" }),
            time: 1,
          },
        },
        {
          type: "message.part.updated",
          properties: {
            sessionID: rootOpenCodeSessionId,
            part: makeTaskPart({ childId, partId: "resume-part-1", status: "completed" }),
            time: 2,
          },
        },
        {
          type: "message.part.updated",
          properties: {
            sessionID: rootOpenCodeSessionId,
            part: makeTaskPart({ childId, partId: "resume-part-2", status: "running" }),
            time: 3,
          },
        },
      ];

      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => isChildActivity(event, childId)),
        Stream.take(3),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
      });

      const events = Array.from(yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second")));
      NodeAssert.equal(events.filter((event) => event.type === "task.started").length, 1);
      NodeAssert.equal(
        events.some(
          (event) =>
            event.type === "task.updated" &&
            (event.payload as { status?: string }).status === "running",
        ),
        true,
      );
      NodeAssert.equal(
        events.every((event) => String((event.payload as { taskId?: unknown }).taskId) === childId),
        true,
      );
      NodeAssert.equal(events.filter((event) => event.type === "task.completed").length, 1);
      NodeAssert.equal(runtimeMock.state.sessionStatusCalls.length, 2);
    }),
  );

  it.effect("keeps reactivated idle children resumable after prior completion", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-opencode-child-resume-idle");
      const childId = "child-resume-idle";
      runtimeMock.state.childMessages.set(childId, [
        {
          info: {
            id: "previous-assistant",
            role: "assistant",
            time: { created: 1, completed: 2 },
          },
          parts: [],
        },
      ]);
      runtimeMock.state.sessionStatusMap = { [childId]: { type: "idle" } };
      runtimeMock.state.subscribedEvents = [
        {
          type: "message.part.updated",
          properties: {
            sessionID: rootOpenCodeSessionId,
            part: makeTaskPart({ childId, partId: "resume-idle-part-1", status: "running" }),
            time: 1,
          },
        },
        {
          type: "message.part.updated",
          properties: {
            sessionID: rootOpenCodeSessionId,
            part: makeTaskPart({ childId, partId: "resume-idle-part-1", status: "completed" }),
            time: 2,
          },
        },
        {
          type: "message.part.updated",
          properties: {
            sessionID: rootOpenCodeSessionId,
            part: makeTaskPart({ childId, partId: "resume-idle-part-2", status: "running" }),
            time: 3,
          },
        },
        childSessionStatus(childId, "busy", "resume-idle-busy"),
      ];

      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => isChildActivity(event, childId)),
        Stream.take(6),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
      });

      const events = Array.from(yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second")));
      NodeAssert.deepEqual(events.map((event) => event.type), [
        "task.started",
        "task.updated",
        "task.completed",
        "task.updated",
        "task.updated",
        "task.updated",
      ]);
      NodeAssert.deepEqual(
        events
          .filter((event) => event.type === "task.updated")
          .map((event) => (event.payload as { status?: string }).status),
        ["idle", "running", "idle", "running"],
      );
    }),
  );

  it.effect("keeps child terminal failures first during late success and idle noise", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-opencode-child-terminal-race");
      const childId = "child-terminal-race";
      const childError = {
        id: "terminal-race-error",
        type: "session.error",
        properties: {
          sessionID: childId,
          error: { data: { message: "child failed first" } },
        },
      };
      runtimeMock.state.sessionStatusMap = { [childId]: { type: "busy" } };
      runtimeMock.state.subscribedEvents = [
        {
          type: "message.part.updated",
          properties: {
            sessionID: rootOpenCodeSessionId,
            part: makeTaskPart({ childId, partId: "terminal-race-part", status: "running" }),
            time: 1,
          },
        },
        childError,
        {
          type: "message.part.updated",
          properties: {
            sessionID: childId,
            part: {
              id: "late-tool-part",
              sessionID: childId,
              messageID: "late-tool-message",
              type: "tool",
              callID: "late-tool-call",
              tool: "bash",
              state: {
                status: "running",
                input: { command: "printf late" },
                title: "Late child tool",
                time: { start: 3 },
              },
            },
          },
        },
        childError,
        {
          type: "message.part.updated",
          properties: {
            sessionID: rootOpenCodeSessionId,
            part: makeTaskPart({ childId, partId: "terminal-race-part", status: "completed" }),
            time: 2,
          },
        },
        childSessionStatus(childId, "idle", "terminal-race-idle"),
      ];

      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => isChildActivity(event, childId)),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
      });

      const events = Array.from(yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second")));
      const terminals = events.filter((event) => event.type === "task.completed");
      NodeAssert.equal(terminals.length, 1);
      NodeAssert.equal(events.some((event) => event.type === "tool.progress"), false);
      NodeAssert.equal(
        terminals[0]?.type === "task.completed" && terminals[0].payload.status === "failed",
        true,
      );
    }),
  );

  it.effect("aborts every tracked child but claims stopped only for acknowledged idle children", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-opencode-child-stop-truth");
      const stoppedChild = "child-stop-ack";
      const liveChild = "child-stop-failed";
      runtimeMock.state.sessionStatusMap = {
        [stoppedChild]: { type: "busy" },
        [liveChild]: { type: "busy" },
      };
      runtimeMock.state.subscribedEvents = [
        {
          type: "message.part.updated",
          properties: {
            sessionID: rootOpenCodeSessionId,
            part: makeTaskPart({ childId: stoppedChild, partId: "stop-part-a", status: "running" }),
            time: 1,
          },
        },
        {
          type: "message.part.updated",
          properties: {
            sessionID: rootOpenCodeSessionId,
            part: makeTaskPart({ childId: liveChild, partId: "stop-part-b", status: "running" }),
            time: 2,
          },
        },
      ];

      const startsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "task.started" &&
            (String((event.payload as { taskId?: unknown }).taskId) === stoppedChild ||
              String((event.payload as { taskId?: unknown }).taskId) === liveChild),
        ),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
      });
      yield* Fiber.join(startsFiber).pipe(Effect.timeout("1 second"));

      runtimeMock.state.sessionStatusMap = {
        [stoppedChild]: { type: "idle" },
        [liveChild]: { type: "busy" },
      };
      runtimeMock.state.abortFailures.add(liveChild);
      const stoppedFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.type === "task.completed"),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* adapter.interruptTurn(threadId);
      const stoppedEvents = Array.from(
        yield* Fiber.join(stoppedFiber).pipe(Effect.timeout("1 second")),
      );

      NodeAssert.equal(runtimeMock.state.abortCalls.includes(rootOpenCodeSessionId), true);
      NodeAssert.equal(runtimeMock.state.abortCalls.includes(stoppedChild), true);
      NodeAssert.equal(runtimeMock.state.abortCalls.includes(liveChild), true);
      NodeAssert.equal(stoppedEvents.length, 1);
      NodeAssert.equal(
        stoppedEvents[0]?.type === "task.completed" &&
          String(stoppedEvents[0].payload.taskId) === stoppedChild &&
          stoppedEvents[0].payload.status === "stopped",
        true,
      );
    }),
  );

  it.effect("hydrates pre-link child events once and deduplicates replayed tool/status events", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-opencode-child-hydration");
      const childId = "child-hydration";
      const childToolPart = {
        id: "hydration-tool-part",
        sessionID: childId,
        messageID: "hydration-message",
        type: "tool",
        callID: "hydration-tool-call",
        tool: "bash",
        state: {
          status: "running",
          input: { command: "printf hydration" },
          title: "Hydrating child tool",
          time: { start: 4 },
        },
      };
      runtimeMock.state.childMessages.set(childId, [
        {
          info: { id: "hydration-message", role: "assistant", time: { created: 1 } },
          parts: [childToolPart],
        },
      ]);
      runtimeMock.state.sessionStatusMap = { [childId]: { type: "busy" } };
      runtimeMock.state.subscribedEvents = [
        childSessionCreated("foreign-child", "foreign-root"),
        {
          type: "message.part.updated",
          properties: {
            sessionID: "foreign-child",
            part: childToolPart,
            time: 1,
          },
        },
        childSessionCreated(childId),
        {
          type: "message.updated",
          properties: {
            sessionID: childId,
            info: { id: "hydration-message", role: "assistant" },
          },
        },
        {
          type: "message.part.updated",
          properties: {
            sessionID: childId,
            part: childToolPart,
            time: 4,
          },
        },
        childSessionStatus(childId, "busy", "hydration-status"),
        {
          type: "message.part.updated",
          properties: {
            sessionID: rootOpenCodeSessionId,
            part: makeTaskPart({ childId, partId: "hydration-root-part", status: "running" }),
            time: 5,
          },
        },
      ];

      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => isChildActivity(event, childId)),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
      });

      const events = Array.from(yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second")));
      NodeAssert.deepEqual(events.map((event) => event.type), ["task.started", "tool.progress"]);
      const started = events[0];
      NodeAssert.equal(started?.type === "task.started" && started.payload.title, "Review adapter behavior");
      NodeAssert.equal(started?.type === "task.started" && started.payload.role, "reviewer");
      NodeAssert.equal(
        started?.type === "task.started" && started.payload.timelineBypass,
        true,
      );
      NodeAssert.deepEqual(runtimeMock.state.messageCalls, [childId]);
      NodeAssert.equal(runtimeMock.state.sessionStatusCalls.length, 1);
    }),
  );

  it.effect("lets OpenCode own session title generation and emits title metadata updates", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-opencode-title-sync");
      runtimeMock.state.subscribedEvents = [
        {
          type: "session.updated",
          properties: {
            info: {
              id: "http://127.0.0.1:9999/session",
              title: "Investigate OpenCode title sync",
            },
          },
        },
      ];

      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.take(3),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
      });

      const events = Array.from(yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second")));
      NodeAssert.equal(runtimeMock.state.sessionCreateInputs.length, 1);
      NodeAssert.equal("title" in (runtimeMock.state.sessionCreateInputs[0] ?? {}), false);

      const metadataUpdated = events.find((event) => event.type === "thread.metadata.updated");
      NodeAssert.ok(metadataUpdated);
      if (metadataUpdated.type === "thread.metadata.updated") {
        NodeAssert.equal(metadataUpdated.payload.name, "Investigate OpenCode title sync");
      }
    }),
  );

  it.effect("writes provider-native observability records using the session thread id", () =>
    Effect.gen(function* () {
      const nativeEvents: Array<{
        readonly event?: {
          readonly provider?: string;
          readonly threadId?: string;
          readonly providerThreadId?: string;
          readonly type?: string;
        };
      }> = [];
      const nativeThreadIds: Array<string | null> = [];
      runtimeMock.state.subscribedEvents = [
        {
          type: "message.updated",
          properties: {
            info: {
              id: "msg-missing-session",
              role: "assistant",
            },
          },
        },
        {
          type: "message.updated",
          properties: {
            sessionID: "http://127.0.0.1:9999/other-session",
            info: {
              id: "msg-other-session",
              role: "assistant",
            },
          },
        },
        {
          type: "message.updated",
          properties: {
            sessionID: "http://127.0.0.1:9999/session",
            info: {
              id: "msg-native-log",
              role: "assistant",
            },
          },
        },
      ];

      const nativeEventLogger = {
        filePath: "memory://opencode-native-events",
        write: (event: unknown, threadId: ThreadId | null) => {
          nativeEvents.push(event as (typeof nativeEvents)[number]);
          nativeThreadIds.push(threadId ?? null);
          return Effect.void;
        },
        close: () => Effect.void,
      };

      const adapterLayer = Layer.effect(
        OpenCodeAdapter,
        makeOpenCodeAdapter(openCodeAdapterTestSettings, {
          nativeEventLogger,
        }),
      ).pipe(
        Layer.provideMerge(Layer.succeed(OpenCodeRuntime, OpenCodeRuntimeTestDouble)),
        Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
        Layer.provideMerge(
          ServerSettingsService.layerTest({
            providers: {
              opencode: {
                binaryPath: "fake-opencode",
                serverUrl: "http://127.0.0.1:9999",
                serverPassword: "secret-password",
              },
            },
          }),
        ),
        Layer.provideMerge(providerSessionDirectoryTestLayer),
        Layer.provideMerge(NodeServices.layer),
      );

      const session = yield* Effect.gen(function* () {
        const adapter = yield* OpenCodeAdapter;
        const started = yield* adapter.startSession({
          provider: ProviderDriverKind.make("opencode"),
          threadId: asThreadId("thread-native-log"),
          runtimeMode: "full-access",
        });
        yield* advanceTestClock(10);
        return started;
      }).pipe(Effect.provide(adapterLayer));

      NodeAssert.equal(session.threadId, "thread-native-log");
      NodeAssert.equal(nativeEvents.length, 1);
      NodeAssert.equal(
        nativeEvents.some((record) => record.event?.provider === "opencode"),
        true,
      );
      NodeAssert.equal(
        nativeEvents.some(
          (record) => record.event?.providerThreadId === "http://127.0.0.1:9999/session",
        ),
        true,
      );
      NodeAssert.equal(
        nativeEvents.some((record) => record.event?.threadId === "thread-native-log"),
        true,
      );
      NodeAssert.equal(
        nativeEvents.some((record) => record.event?.type === "message.updated"),
        true,
      );
      NodeAssert.equal(
        nativeThreadIds.every((threadId) => threadId === "thread-native-log"),
        true,
      );
    }),
  );

  it.effect("keeps the event pump alive when native event logging fails", () =>
    Effect.gen(function* () {
      runtimeMock.state.subscribedEvents = [
        {
          type: "message.updated",
          properties: {
            sessionID: "http://127.0.0.1:9999/session",
            info: {
              id: "msg-native-log-failure",
              role: "assistant",
            },
          },
        },
      ];

      const nativeEventLogger = {
        filePath: "memory://opencode-native-events",
        write: () => Effect.die(new Error("native log write failed")),
        close: () => Effect.void,
      };

      const adapterLayer = Layer.effect(
        OpenCodeAdapter,
        makeOpenCodeAdapter(openCodeAdapterTestSettings, {
          nativeEventLogger,
        }),
      ).pipe(
        Layer.provideMerge(Layer.succeed(OpenCodeRuntime, OpenCodeRuntimeTestDouble)),
        Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
        Layer.provideMerge(
          ServerSettingsService.layerTest({
            providers: {
              opencode: {
                binaryPath: "fake-opencode",
                serverUrl: "http://127.0.0.1:9999",
                serverPassword: "secret-password",
              },
            },
          }),
        ),
        Layer.provideMerge(providerSessionDirectoryTestLayer),
        Layer.provideMerge(NodeServices.layer),
      );

      // Capture closeCalls *inside* the provided layer scope: the adapter's
      // layer finalizer now tears down any live sessions when the layer
      // closes (which is exactly what we want for leak prevention), so
      // inspecting closeCalls after `Effect.provide` completes would observe
      // the teardown — not the behavior under test. We care that the event
      // pump kept the session alive while logging was failing.
      const { sessions, closeCallsDuringRun } = yield* Effect.gen(function* () {
        const adapter = yield* OpenCodeAdapter;
        yield* adapter.startSession({
          provider: ProviderDriverKind.make("opencode"),
          threadId: asThreadId("thread-native-log-failure"),
          runtimeMode: "full-access",
        });
        yield* advanceTestClock(10);
        return {
          sessions: yield* adapter.listSessions(),
          closeCallsDuringRun: [...runtimeMock.state.closeCalls],
        };
      }).pipe(Effect.provide(adapterLayer));

      NodeAssert.equal(sessions.length, 1);
      NodeAssert.equal(sessions[0]?.threadId, "thread-native-log-failure");
      NodeAssert.deepEqual(closeCallsDuringRun, []);
    }),
  );
});
