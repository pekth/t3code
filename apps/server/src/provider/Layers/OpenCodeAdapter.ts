import {
  EventId,
  type OpenCodeSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  RuntimeItemId,
  RuntimeRequestId,
  RuntimeTaskId,
  ThreadId,
  type RuntimeTaskStatus,
  type ToolLifecycleItemType,
  TurnId,
  type UserInputQuestion,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import type { OpencodeClient, Part, PermissionRequest, QuestionRequest } from "@opencode-ai/sdk/v2";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import { type EventNdjsonLogger, makeEventNdjsonLogger } from "./EventNdjsonLogger.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionClosedError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import { type OpenCodeAdapterShape } from "../Services/OpenCodeAdapter.ts";
import {
  buildOpenCodePermissionRules,
  OpenCodeRuntime,
  OpenCodeRuntimeError,
  openCodeQuestionId,
  openCodeRuntimeErrorDetail,
  parseOpenCodeModelSlug,
  runOpenCodeSdk,
  toOpenCodeFileParts,
  toOpenCodePermissionReply,
  toOpenCodeQuestionAnswers,
  type OpenCodeServerConnection,
} from "../opencodeRuntime.ts";
import * as Option from "effect/Option";

const PROVIDER = ProviderDriverKind.make("opencode");

/**
 * Version tag stamped into the OpenCode resume cursor. Bump if the cursor
 * shape changes so stale-shaped cursors written by older builds are ignored
 * rather than misread (mirrors GROK_RESUME_VERSION / CURSOR_RESUME_VERSION).
 */
const OPENCODE_RESUME_VERSION = 1 as const;

/**
 * Decode a persisted resume cursor into the upstream `ses_…` id. Anything
 * that isn't a current-version cursor with a non-empty id means "no resume"
 * rather than an error. Re-adopting the session id IS the resume mechanism —
 * OpenCode scopes a conversation's history by session id.
 */
function parseOpenCodeResume(raw: unknown): { readonly sessionId: string } | undefined {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    return undefined;
  }
  const record = raw as Record<string, unknown>;
  if (record.schemaVersion !== OPENCODE_RESUME_VERSION) {
    return undefined;
  }
  if (typeof record.sessionId !== "string" || record.sessionId.trim().length === 0) {
    return undefined;
  }
  return { sessionId: record.sessionId.trim() };
}

/**
 * Whether an error definitively reports a missing session. Only a confirmed
 * miss may silently start a fresh session; any other failure (the SDK client
 * is `throwOnError: true`, so `session.get` rejects on every non-2xx) must
 * propagate, or a transient blip resets a live thread to an empty one — the
 * #3604 silent context loss. Decides on structured signals only, never free
 * text: a numeric 404 or the exact `NotFoundError` name, found via a bounded walk
 * over `cause`/`body`/`error`/`data`. An explicit non-404 status seals its
 * subtree so a wrapped "NotFound" name can't reclassify a real failure.
 * Exported for unit testing.
 */
export function isOpenCodeNotFound(cause: unknown): boolean {
  const seen = new Set<unknown>();
  const queue: Array<unknown> = [cause];
  for (let steps = 0; queue.length > 0 && steps < 32; steps += 1) {
    const node = queue.shift();
    if (node === null || typeof node !== "object" || seen.has(node)) {
      continue;
    }
    seen.add(node);
    const record = node as Record<string, unknown>;

    const response = record.response;
    const statuses = [
      record.status,
      record.statusCode,
      response !== null && typeof response === "object"
        ? (response as { readonly status?: unknown }).status
        : undefined,
    ].filter((status): status is number => typeof status === "number");
    if (statuses.includes(404)) {
      return true;
    }
    if (statuses.length > 0) {
      continue;
    }

    const name = record.name;
    if (typeof name === "string" && name.toLowerCase() === "notfounderror") {
      return true;
    }

    for (const key of ["cause", "body", "error", "data"] as const) {
      if (record[key] !== undefined) {
        queue.push(record[key]);
      }
    }
  }
  return false;
}

/**
 * Whether two directory spellings name the same location. Raw string
 * equality misreads a trailing slash, `.`/`..` segment, or symlinked cwd
 * (macOS `/tmp` → `/private/tmp`) as a cwd change, needlessly forking the
 * session on every resume. Lexically equal paths short-circuit; otherwise
 * both sides go through `realPath`, each falling back to its lexical form
 * on failure (deleted directory, external-server path) — so the probe can
 * only widen matches, never split them. Takes the services as arguments so
 * adapter methods stay service-free. Exported for unit testing.
 */
export function isSameOpenCodeDirectory(
  fileSystem: FileSystem.FileSystem,
  path: Path.Path,
  left: string,
  right: string,
): Effect.Effect<boolean> {
  const lexicalLeft = path.resolve(left);
  const lexicalRight = path.resolve(right);
  if (lexicalLeft === lexicalRight) {
    return Effect.succeed(true);
  }
  const canonicalize = (lexical: string) =>
    fileSystem.realPath(lexical).pipe(Effect.orElseSucceed(() => lexical));
  return Effect.zipWith(
    canonicalize(lexicalLeft),
    canonicalize(lexicalRight),
    (canonicalLeft, canonicalRight) => canonicalLeft === canonicalRight,
  );
}

interface OpenCodeTurnSnapshot {
  readonly id: TurnId;
  readonly items: Array<unknown>;
}

type OpenCodeSubscribedEvent =
  Awaited<ReturnType<OpencodeClient["event"]["subscribe"]>> extends {
    readonly stream: AsyncIterable<infer TEvent>;
  }
    ? TEvent
    : never;

function trimText(value: string | undefined | null): string | undefined {
  const trimmed = value?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : undefined;
}

function openCodeEventSessionId(event: OpenCodeSubscribedEvent): string | undefined {
  const properties = "properties" in event ? event.properties : undefined;
  if (!properties || typeof properties !== "object") {
    return undefined;
  }

  const sessionID = (properties as { readonly sessionID?: unknown }).sessionID;
  const sessionIDFromProperties = typeof sessionID === "string" ? sessionID : undefined;
  if (sessionIDFromProperties) {
    return sessionIDFromProperties;
  }

  const info = (properties as { readonly info?: { readonly id?: unknown } }).info;
  return info && typeof info.id === "string" ? info.id : undefined;
}

function openCodeEventSessionTitle(event: OpenCodeSubscribedEvent): string | undefined {
  if (event.type !== "session.updated") {
    return undefined;
  }

  return trimText(event.properties.info.title);
}

type OpenCodeChildTerminalStatus = "completed" | "failed" | "stopped";

interface OpenCodeTaskLink {
  readonly childId: string;
  readonly parentSessionId: string;
  readonly description: string;
  readonly role: string;
  readonly model?: string;
  readonly background: boolean;
}

interface OpenCodeChildContext {
  readonly id: string;
  readonly parentSessionId: string;
  description: string | undefined;
  role: string | undefined;
  model: string | undefined;
  background: boolean;
  linked: boolean;
  lastTaskPartId: string | undefined;
  lastStatus: RuntimeTaskStatus | undefined;
  terminalStatus: OpenCodeChildTerminalStatus | undefined;
  lastToolFingerprint: string | undefined;
  readonly bufferedEvents: Array<OpenCodeSubscribedEvent>;
  readonly bufferedEventKeys: Set<string>;
  readonly seenEventKeys: Set<string>;
}

const OPENCODE_CHILD_EVENT_BUFFER_LIMIT = 64;
const OPENCODE_CHILD_EVENT_KEY_LIMIT = 256;
const OPENCODE_CHILD_ABORT_TIMEOUT = "5 seconds";

function openCodeRecord(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function openCodeString(value: unknown): string | undefined {
  return typeof value === "string" ? trimText(value) : undefined;
}

function openCodeModelName(value: unknown): string | undefined {
  if (typeof value === "string") {
    return trimText(value);
  }
  const model = openCodeRecord(value);
  const providerID = openCodeString(model?.providerID);
  const modelID = openCodeString(model?.modelID);
  return providerID && modelID ? `${providerID}/${modelID}` : undefined;
}

function openCodeTaskLinkFromPart(part: Part): OpenCodeTaskLink | undefined {
  if (part.type !== "tool" || part.tool !== "task") {
    return undefined;
  }

  const input = openCodeRecord(part.state.input);
  const state = openCodeRecord(part.state);
  const metadata = openCodeRecord(state?.metadata);
  const childId = openCodeString(metadata?.sessionId);
  const parentSessionId = openCodeString(metadata?.parentSessionId);
  const description = openCodeString(input?.description);
  const role = openCodeString(input?.subagent_type);
  if (!childId || !parentSessionId || !description || !role) {
    return undefined;
  }

  const model = openCodeModelName(metadata?.model);
  return {
    childId,
    parentSessionId,
    description,
    role,
    ...(model ? { model } : {}),
    background: metadata?.background === true || input?.background === true,
  };
}

function openCodeProviderStatusType(value: unknown): "busy" | "retry" | "idle" | undefined {
  const type = openCodeString(openCodeRecord(value)?.type);
  return type === "busy" || type === "retry" || type === "idle" ? type : undefined;
}

function openCodeChildEventKey(event: OpenCodeSubscribedEvent): string {
  const eventId = "id" in event ? openCodeString(event.id) : undefined;
  if (eventId) {
    return `${event.type}:${eventId}`;
  }

  const properties = openCodeRecord("properties" in event ? event.properties : undefined);
  const part = openCodeRecord(properties?.part);
  const info = openCodeRecord(properties?.info);
  const status = openCodeString(openCodeRecord(properties?.status)?.type);
  const partState = openCodeRecord(part?.state);
  const partStatus = openCodeString(partState?.status);
  const partTime = openCodeRecord(partState?.time);
  const partStateKey = partStatus
    ? `:${partStatus}:${JSON.stringify(partTime ?? null)}`
    : "";
  const stableId =
    openCodeString(part?.id) ??
    openCodeString(info?.id) ??
    openCodeString(properties?.messageID) ??
    openCodeString(properties?.requestID) ??
    status;
  const infoSnapshot = info ? `:${JSON.stringify(info)}` : "";
  return `${event.type}:${stableId ?? "event"}${partStateKey}${infoSnapshot}`;
}

function openCodeChildLinkage(child: OpenCodeChildContext) {
  if (!child.linked || !child.description || !child.role) {
    return undefined;
  }
  return {
    taskId: RuntimeTaskId.make(child.id),
    taskType: "local_agent" as const,
    description: child.description,
    title: child.description,
    role: child.role,
    ...(child.model ? { model: child.model } : {}),
    timelineBypass: true as const,
  };
}

interface OpenCodeSessionContext {
  session: ProviderSession;
  readonly client: OpencodeClient;
  readonly server: OpenCodeServerConnection;
  readonly directory: string;
  readonly openCodeSessionId: string;
  readonly pendingPermissions: Map<string, PermissionRequest>;
  readonly pendingQuestions: Map<string, QuestionRequest>;
  readonly messageRoleById: Map<string, "user" | "assistant">;
  readonly partById: Map<string, Part>;
  readonly emittedTextByPartId: Map<string, string>;
  readonly completedAssistantPartIds: Set<string>;
  readonly children: Map<string, OpenCodeChildContext>;
  readonly abortingChildIds: Set<string>;
  childStatusMap: Readonly<Record<string, unknown>> | undefined;
  childStatusMapLoaded: boolean;
  emitChildStopped: (childId: string) => Effect.Effect<void>;
  readonly turns: Array<OpenCodeTurnSnapshot>;
  activeTurnId: TurnId | undefined;
  activeAgent: string | undefined;
  activeVariant: string | undefined;
  /**
   * One-shot guard flipped by `stopOpenCodeContext` / `emitUnexpectedExit`.
   * The session lifecycle is owned by `sessionScope`; this Ref exists only
   * so concurrent callers can race the transition safely via `getAndSet`.
   */
  readonly stopped: Ref.Ref<boolean>;
  /**
   * Sole lifecycle handle for the session. Closing this scope:
   *   - aborts the `AbortController` registered as a finalizer
   *     (cancels the in-flight `event.subscribe` fetch),
   *   - interrupts the event-pump and server-exit fibers forked
   *     via `Effect.forkIn(sessionScope)`,
   *   - tears down the OpenCode server process for scope-owned servers.
   */
  readonly sessionScope: Scope.Closeable;
}

export interface OpenCodeAdapterLiveOptions {
  readonly instanceId?: ProviderInstanceId;
  readonly environment?: NodeJS.ProcessEnv;
  readonly nativeEventLogPath?: string;
  readonly nativeEventLogger?: EventNdjsonLogger;
}

const nowIso = Effect.map(DateTime.now, DateTime.formatIso);

/**
 * Map a tagged OpenCodeRuntimeError produced by {@link runOpenCodeSdk} into
 * the adapter-boundary `ProviderAdapterRequestError`. SDK-method-level call
 * sites pipe through this in `Effect.mapError` so they never build the error
 * shape by hand.
 */
const toRequestError = (cause: OpenCodeRuntimeError): ProviderAdapterRequestError =>
  new ProviderAdapterRequestError({
    provider: PROVIDER,
    method: cause.operation,
    detail: cause.detail,
    cause: cause.cause,
  });

/**
 * Map a `Cause.squash`-ed failure into a `ProviderAdapterProcessError`. The
 * typed cause is usually an `OpenCodeRuntimeError` (from {@link runOpenCodeSdk}),
 * in which case we preserve its `detail`; otherwise we fall back to
 * {@link openCodeRuntimeErrorDetail} for unknown causes (defects, etc.).
 */
const toProcessError = (threadId: ThreadId, cause: unknown): ProviderAdapterProcessError =>
  new ProviderAdapterProcessError({
    provider: PROVIDER,
    threadId,
    detail: OpenCodeRuntimeError.is(cause) ? cause.detail : openCodeRuntimeErrorDetail(cause),
    cause,
  });

type EventBaseInput = {
  readonly threadId: ThreadId;
  readonly turnId?: TurnId | undefined;
  readonly itemId?: string | undefined;
  readonly requestId?: string | undefined;
  readonly createdAt?: string | undefined;
  readonly raw?: unknown;
};

function toToolLifecycleItemType(toolName: string): ToolLifecycleItemType {
  const normalized = toolName.toLowerCase();
  if (normalized.includes("bash") || normalized.includes("command")) {
    return "command_execution";
  }
  if (
    normalized.includes("edit") ||
    normalized.includes("write") ||
    normalized.includes("patch") ||
    normalized.includes("multiedit")
  ) {
    return "file_change";
  }
  if (normalized.includes("web")) {
    return "web_search";
  }
  if (normalized.includes("mcp")) {
    return "mcp_tool_call";
  }
  if (normalized.includes("image")) {
    return "image_view";
  }
  if (
    normalized.includes("task") ||
    normalized.includes("agent") ||
    normalized.includes("subtask")
  ) {
    return "collab_agent_tool_call";
  }
  return "dynamic_tool_call";
}

function mapPermissionToRequestType(
  permission: string,
): "command_execution_approval" | "file_read_approval" | "file_change_approval" | "unknown" {
  switch (permission) {
    case "bash":
      return "command_execution_approval";
    case "read":
      return "file_read_approval";
    case "edit":
      return "file_change_approval";
    default:
      return "unknown";
  }
}

function mapPermissionDecision(reply: "once" | "always" | "reject"): string {
  switch (reply) {
    case "once":
      return "accept";
    case "always":
      return "acceptForSession";
    case "reject":
    default:
      return "decline";
  }
}

function resolveTurnSnapshot(
  context: OpenCodeSessionContext,
  turnId: TurnId,
): OpenCodeTurnSnapshot {
  const existing = context.turns.find((turn) => turn.id === turnId);
  if (existing) {
    return existing;
  }

  const created: OpenCodeTurnSnapshot = { id: turnId, items: [] };
  context.turns.push(created);
  return created;
}

function appendTurnItem(
  context: OpenCodeSessionContext,
  turnId: TurnId | undefined,
  item: unknown,
): void {
  if (!turnId) {
    return;
  }
  resolveTurnSnapshot(context, turnId).items.push(item);
}

const ensureSessionContext = Effect.fn("ensureSessionContext")(function* (
  sessions: ReadonlyMap<ThreadId, OpenCodeSessionContext>,
  threadId: ThreadId,
) {
  const session = sessions.get(threadId);
  if (!session) {
    return yield* new ProviderAdapterSessionNotFoundError({
      provider: PROVIDER,
      threadId,
    });
  }
  if (yield* Ref.get(session.stopped)) {
    return yield* new ProviderAdapterSessionClosedError({
      provider: PROVIDER,
      threadId,
    });
  }
  return session;
});

function normalizeQuestionRequest(request: QuestionRequest): ReadonlyArray<UserInputQuestion> {
  return request.questions.map((question, index) => ({
    id: openCodeQuestionId(index, question),
    header: question.header,
    question: question.question,
    options: question.options.map((option) => ({
      label: option.label,
      description: option.description,
    })),
    ...(question.multiple ? { multiSelect: true } : {}),
  }));
}

function resolveTextStreamKind(part: Part | undefined): "assistant_text" | "reasoning_text" {
  return part?.type === "reasoning" ? "reasoning_text" : "assistant_text";
}

function textFromPart(part: Part): string | undefined {
  switch (part.type) {
    case "text":
    case "reasoning":
      return part.text;
    default:
      return undefined;
  }
}

function commonPrefixLength(left: string, right: string): number {
  let index = 0;
  while (index < left.length && index < right.length && left[index] === right[index]) {
    index += 1;
  }
  return index;
}

function resolveLatestAssistantText(previousText: string | undefined, nextText: string): string {
  if (previousText && previousText.length > nextText.length && previousText.startsWith(nextText)) {
    return previousText;
  }
  return nextText;
}

export function mergeOpenCodeAssistantText(
  previousText: string | undefined,
  nextText: string,
): {
  readonly latestText: string;
  readonly deltaToEmit: string;
} {
  const latestText = resolveLatestAssistantText(previousText, nextText);
  return {
    latestText,
    deltaToEmit: latestText.slice(commonPrefixLength(previousText ?? "", latestText)),
  };
}

export function appendOpenCodeAssistantTextDelta(
  previousText: string,
  delta: string,
): {
  readonly nextText: string;
  readonly deltaToEmit: string;
} {
  return {
    nextText: previousText + delta,
    deltaToEmit: delta,
  };
}

const isoFromEpochMs = (value: number) =>
  DateTime.make(value).pipe(
    Option.match({
      onNone: () => undefined,
      onSome: DateTime.formatIso,
    }),
  );

function messageRoleForPart(
  context: OpenCodeSessionContext,
  part: Pick<Part, "messageID" | "type">,
): "assistant" | "user" | undefined {
  const known = context.messageRoleById.get(part.messageID);
  if (known) {
    return known;
  }
  return part.type === "tool" ? "assistant" : undefined;
}

function detailFromToolPart(part: Extract<Part, { type: "tool" }>): string | undefined {
  switch (part.state.status) {
    case "completed":
      return part.state.output;
    case "error":
      return part.state.error;
    case "running":
      return part.state.title;
    default:
      return undefined;
  }
}

function toolStateCreatedAt(part: Extract<Part, { type: "tool" }>): string | undefined {
  switch (part.state.status) {
    case "running":
      return isoFromEpochMs(part.state.time.start);
    case "completed":
    case "error":
      return isoFromEpochMs(part.state.time.end);
    default:
      return undefined;
  }
}

function sessionErrorMessage(error: unknown): string {
  if (!error || typeof error !== "object") {
    return "OpenCode session failed.";
  }
  const data = "data" in error && error.data && typeof error.data === "object" ? error.data : null;
  const message = data && "message" in data ? data.message : null;
  return typeof message === "string" && message.trim().length > 0
    ? message
    : "OpenCode session failed.";
}

function updateProviderSession(
  context: OpenCodeSessionContext,
  patch: Partial<ProviderSession>,
  options?: {
    readonly clearActiveTurnId?: boolean;
    readonly clearLastError?: boolean;
  },
): Effect.Effect<ProviderSession> {
  return Effect.gen(function* () {
    const updatedAt = yield* nowIso;
    const nextSession = {
      ...context.session,
      ...patch,
      updatedAt,
    } as ProviderSession & Record<string, unknown>;
    const mutableSession = nextSession as Record<string, unknown>;
    if (options?.clearActiveTurnId) {
      delete mutableSession.activeTurnId;
    }
    if (options?.clearLastError) {
      delete mutableSession.lastError;
    }
    context.session = nextSession;
    return nextSession;
  });
}

const abortTrackedOpenCodeChildren = Effect.fn("abortTrackedOpenCodeChildren")(function* (
  context: OpenCodeSessionContext,
) {
  const childIds = [...context.children.keys()];
  if (childIds.length === 0) {
    return [] as string[];
  }
  for (const childId of childIds) {
    context.abortingChildIds.add(childId);
  }

  const attempts = yield* Effect.forEach(
    childIds,
    (childId) =>
      runOpenCodeSdk("session.abort", () => context.client.session.abort({ sessionID: childId })).pipe(
        Effect.timeout(OPENCODE_CHILD_ABORT_TIMEOUT),
        Effect.map(() => ({ childId, aborted: true })),
        Effect.catch(() => Effect.succeed({ childId, aborted: false })),
      ),
    { concurrency: "unbounded" },
  );
  const abortedIds = attempts.filter((attempt) => attempt.aborted).map((attempt) => attempt.childId);
  if (abortedIds.length === 0) {
    clearAbortingOpenCodeChildren(context, childIds);
    return [] as string[];
  }

  const statusMap = yield* runOpenCodeSdk("session.status", () =>
    context.client.session.status({ directory: context.directory }),
  ).pipe(
    Effect.map((response) => openCodeRecord(response.data)),
    Effect.catch(() => Effect.succeed(undefined)),
  );
  if (!statusMap) {
    clearAbortingOpenCodeChildren(context, childIds);
    return [] as string[];
  }

  const stoppedChildIds = abortedIds.filter((childId) => {
    const child = context.children.get(childId);
    if (!child || child.terminalStatus) {
      return false;
    }
    const status = statusMap[childId];
    return status === undefined || openCodeProviderStatusType(status) === "idle";
  });
  clearAbortingOpenCodeChildren(
    context,
    childIds.filter((childId) => !stoppedChildIds.includes(childId)),
  );
  return stoppedChildIds;
});

const clearAbortingOpenCodeChildren = (
  context: OpenCodeSessionContext,
  childIds: ReadonlyArray<string>,
): void => {
  for (const childId of childIds) {
    context.abortingChildIds.delete(childId);
  }
};

const stopOpenCodeContext = Effect.fn("stopOpenCodeContext")(function* (
  context: OpenCodeSessionContext,
) {
  // Race-safe one-shot: first caller flips the flag, everyone else no-ops.
  if (yield* Ref.getAndSet(context.stopped, true)) {
    return false;
  }

  // Capture and abort every tracked direct child before closing the scope.
  // A stopped row is emitted only when the child abort succeeds and the
  // provider no longer reports it busy/retrying.
  const stoppedChildIds = yield* abortTrackedOpenCodeChildren(context);
  yield* Effect.forEach(
    stoppedChildIds,
    (childId) => context.emitChildStopped(childId).pipe(Effect.ignore),
    { concurrency: "unbounded", discard: true },
  );
  clearAbortingOpenCodeChildren(context, stoppedChildIds);

  // Best-effort remote abort. The scope close below tears down the local
  // handles (event-pump fiber, server-exit fiber, event-subscribe fetch),
  // but we still want to tell OpenCode that this session is done.
  yield* runOpenCodeSdk("session.abort", () =>
    context.client.session.abort({ sessionID: context.openCodeSessionId }),
  ).pipe(Effect.ignore({ log: true }));

  // Closing the session scope interrupts every fiber forked into it and
  // runs each finalizer we registered — the `AbortController.abort()` call,
  // the child-process termination, etc.
  yield* Scope.close(context.sessionScope, Exit.void);
  return true;
});

export function makeOpenCodeAdapter(
  openCodeSettings: OpenCodeSettings,
  options?: OpenCodeAdapterLiveOptions,
) {
  return Effect.gen(function* () {
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("opencode");
    const serverConfig = yield* ServerConfig;
    const openCodeRuntime = yield* OpenCodeRuntime;
    const crypto = yield* Crypto.Crypto;
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const sameDirectory = (left: string, right: string) =>
      isSameOpenCodeDirectory(fileSystem, path, left, right);
    const nativeEventLogger =
      options?.nativeEventLogger ??
      (options?.nativeEventLogPath !== undefined
        ? yield* makeEventNdjsonLogger(options.nativeEventLogPath, {
            stream: "native",
          })
        : undefined);
    // Only close loggers we created. If the caller passed one in via
    // `options.nativeEventLogger`, they own its lifecycle.
    const managedNativeEventLogger =
      options?.nativeEventLogger === undefined ? nativeEventLogger : undefined;
    const runtimeEvents = yield* Queue.unbounded<ProviderRuntimeEvent>();
    const sessions = new Map<ThreadId, OpenCodeSessionContext>();
    const randomUUIDv4 = crypto.randomUUIDv4.pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "crypto/randomUUIDv4",
            detail: "Failed to generate OpenCode runtime identifier.",
            cause,
          }),
      ),
    );
    const buildEventBase = (input: EventBaseInput) =>
      Effect.all({
        eventId: randomUUIDv4.pipe(Effect.map(EventId.make)),
        createdAt: input.createdAt === undefined ? nowIso : Effect.succeed(input.createdAt),
      }).pipe(
        Effect.map(({ eventId, createdAt }) => ({
          eventId,
          provider: PROVIDER,
          threadId: input.threadId,
          createdAt,
          ...(input.turnId ? { turnId: input.turnId } : {}),
          ...(input.itemId ? { itemId: RuntimeItemId.make(input.itemId) } : {}),
          ...(input.requestId ? { requestId: RuntimeRequestId.make(input.requestId) } : {}),
          ...(input.raw !== undefined
            ? {
                raw: {
                  source: "opencode.sdk.event" as const,
                  payload: input.raw,
                },
              }
            : {}),
        })),
      );

    // Layer-level finalizer: when the adapter layer shuts down, stop every
    // session. Each session's `Scope.close` tears down its spawned OpenCode
    // server (via the `ChildProcessSpawner` finalizer installed in
    // `startOpenCodeServerProcess`) and interrupts the forked event/exit
    // fibers. Consumers that can't reason about Effect scopes therefore
    // cannot leak OpenCode child processes by forgetting to call `stopAll`.
    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        const contexts = [...sessions.values()];
        sessions.clear();
        // `ignoreCause` swallows both typed failures (none here) and defects
        // from throwing scope finalizers so a sibling's death can't interrupt
        // the remaining cleanups.
        yield* Effect.forEach(
          contexts,
          (context) => Effect.ignoreCause(stopOpenCodeContext(context)),
          { concurrency: "unbounded", discard: true },
        );
        // Close the logger AFTER session teardown so any final lifecycle
        // events emitted during shutdown still get written. `close` flushes
        // the `Logger.batched` window and closes each per-thread
        // `RotatingFileSink` handle owned by the logger's internal scope.
        if (managedNativeEventLogger !== undefined) {
          yield* managedNativeEventLogger.close();
        }
      }).pipe(Effect.ensuring(Queue.shutdown(runtimeEvents))),
    );

    const emit = (event: ProviderRuntimeEvent) =>
      Queue.offer(runtimeEvents, event).pipe(Effect.asVoid);
    const writeNativeEvent = (
      threadId: ThreadId,
      event: {
        readonly observedAt: string;
        readonly event: Record<string, unknown>;
      },
    ) => (nativeEventLogger ? nativeEventLogger.write(event, threadId) : Effect.void);
    const writeNativeEventBestEffort = (
      threadId: ThreadId,
      event: {
        readonly observedAt: string;
        readonly event: Record<string, unknown>;
      },
    ) => writeNativeEvent(threadId, event).pipe(Effect.catchCause(() => Effect.void));

    const emitUnexpectedExit = Effect.fn("emitUnexpectedExit")(function* (
      context: OpenCodeSessionContext,
      message: string,
    ) {
      // Atomic one-shot: two fibers can race here (the event-pump on stream
      // failure and the server-exit watcher). `getAndSet` flips the flag in
      // a single step so the loser observes `true` and returns; a plain
      // `Ref.get` would let both racers slip past and emit duplicates.
      if (yield* Ref.getAndSet(context.stopped, true)) {
        return;
      }
      const turnId = context.activeTurnId;
      sessions.delete(context.session.threadId);
      // Emit lifecycle events BEFORE tearing down the scope. Both call sites
      // run this inside a fiber forked via `Effect.forkIn(context.sessionScope)`;
      // closing that scope triggers the fiber-interrupt finalizer, so any
      // subsequent yield point would unwind and silently drop these emits.
      yield* emit({
        ...(yield* buildEventBase({
          threadId: context.session.threadId,
          turnId,
        })),
        type: "runtime.error",
        payload: {
          message,
          class: "transport_error",
        },
      }).pipe(Effect.ignore);
      yield* emit({
        ...(yield* buildEventBase({
          threadId: context.session.threadId,
          turnId,
        })),
        type: "session.exited",
        payload: {
          reason: message,
          recoverable: false,
          exitKind: "error",
        },
      }).pipe(Effect.ignore);
      // Inline the teardown that `stopOpenCodeContext` would do; we can't
      // delegate to it because our `getAndSet` above already flipped the
      // one-shot guard, so the call would no-op.
      const stoppedChildIds = yield* abortTrackedOpenCodeChildren(context);
      yield* Effect.forEach(
        stoppedChildIds,
        (childId) => context.emitChildStopped(childId).pipe(Effect.ignore),
        { concurrency: "unbounded", discard: true },
      );
      clearAbortingOpenCodeChildren(context, stoppedChildIds);
      yield* runOpenCodeSdk("session.abort", () =>
        context.client.session.abort({ sessionID: context.openCodeSessionId }),
      ).pipe(Effect.ignore({ log: true }));
      yield* Scope.close(context.sessionScope, Exit.void);
    });

    /** Emit content.delta and item.completed events for an assistant text part. */
    const emitAssistantTextDelta = Effect.fn("emitAssistantTextDelta")(function* (
      context: OpenCodeSessionContext,
      part: Part,
      turnId: TurnId | undefined,
      raw: unknown,
    ) {
      const text = textFromPart(part);
      if (text === undefined) {
        return;
      }
      const previousText = context.emittedTextByPartId.get(part.id);
      const { latestText, deltaToEmit } = mergeOpenCodeAssistantText(previousText, text);
      context.emittedTextByPartId.set(part.id, latestText);
      if (latestText !== text) {
        context.partById.set(
          part.id,
          (part.type === "text" || part.type === "reasoning"
            ? { ...part, text: latestText }
            : part) satisfies Part,
        );
      }
      if (deltaToEmit.length > 0) {
        yield* emit({
          ...(yield* buildEventBase({
            threadId: context.session.threadId,
            turnId,
            itemId: part.id,
            createdAt:
              (part.type === "text" || part.type === "reasoning") && part.time !== undefined
                ? isoFromEpochMs(part.time.start)
                : undefined,
            raw,
          })),
          type: "content.delta",
          payload: {
            streamKind: resolveTextStreamKind(part),
            delta: deltaToEmit,
          },
        });
      }

      if (
        part.type === "text" &&
        part.time?.end !== undefined &&
        !context.completedAssistantPartIds.has(part.id)
      ) {
        context.completedAssistantPartIds.add(part.id);
        yield* emit({
          ...(yield* buildEventBase({
            threadId: context.session.threadId,
            turnId,
            itemId: part.id,
            createdAt: isoFromEpochMs(part.time.end),
            raw,
          })),
          type: "item.completed",
          payload: {
            itemType: "assistant_message",
            status: "completed",
            title: "Assistant message",
            ...(latestText.length > 0 ? { detail: latestText } : {}),
          },
        });
      }
    });

    const rememberChildEventKey = (
      child: OpenCodeChildContext,
      event: OpenCodeSubscribedEvent,
    ): boolean => {
      const key = openCodeChildEventKey(event);
      if (child.seenEventKeys.has(key)) {
        return false;
      }
      if (child.seenEventKeys.size >= OPENCODE_CHILD_EVENT_KEY_LIMIT) {
        const oldest = child.seenEventKeys.values().next().value;
        if (typeof oldest === "string") {
          child.seenEventKeys.delete(oldest);
        }
      }
      child.seenEventKeys.add(key);
      return true;
    };

    const bufferChildEvent = (child: OpenCodeChildContext, event: OpenCodeSubscribedEvent): void => {
      const key = openCodeChildEventKey(event);
      if (child.bufferedEventKeys.has(key)) {
        return;
      }
      if (child.bufferedEvents.length >= OPENCODE_CHILD_EVENT_BUFFER_LIMIT) {
        return;
      }
      child.bufferedEvents.push(event);
      child.bufferedEventKeys.add(key);
    };

    const emitChildStatus = Effect.fn("emitChildStatus")(function* (
      context: OpenCodeSessionContext,
      child: OpenCodeChildContext,
      status: RuntimeTaskStatus,
      turnId: TurnId | undefined,
      raw?: unknown,
    ) {
      if (yield* Ref.get(context.stopped)) {
        return;
      }
      const linkage = openCodeChildLinkage(child);
      if (
        !linkage ||
        context.abortingChildIds.has(child.id) ||
        child.terminalStatus ||
        child.lastStatus === status
      ) {
        return;
      }
      child.lastStatus = status;
      yield* emit({
        ...(yield* buildEventBase({
          threadId: context.session.threadId,
          turnId,
          raw,
        })),
        type: "task.updated",
        payload: {
          ...linkage,
          status,
        },
      });
    });

    const emitChildStarted = Effect.fn("emitChildStarted")(function* (
      context: OpenCodeSessionContext,
      child: OpenCodeChildContext,
      turnId: TurnId | undefined,
      raw?: unknown,
    ) {
      if (yield* Ref.get(context.stopped)) {
        return;
      }
      const linkage = openCodeChildLinkage(child);
      if (!linkage || context.abortingChildIds.has(child.id)) {
        return;
      }
      child.lastStatus = "running";
      const eventBase = yield* buildEventBase({
        threadId: context.session.threadId,
        turnId,
        raw,
      });
      if (yield* Ref.get(context.stopped)) {
        return;
      }
      yield* emit({
        ...eventBase,
        type: "task.started",
        payload: linkage,
      });
    });

    const emitChildToolProgress = Effect.fn("emitChildToolProgress")(function* (
      context: OpenCodeSessionContext,
      child: OpenCodeChildContext,
      part: Extract<Part, { type: "tool" }>,
      turnId: TurnId | undefined,
      raw?: unknown,
    ) {
      if (yield* Ref.get(context.stopped)) {
        return;
      }
      const linkage = openCodeChildLinkage(child);
      if (!linkage || context.abortingChildIds.has(child.id) || child.terminalStatus) {
        return;
      }
      const fingerprint = `${part.id}:${part.tool}:${part.state.status}:${
        part.state.status === "running"
          ? part.state.time.start
          : part.state.status === "pending"
            ? part.state.raw
            : part.state.time.end
      }`;
      if (child.lastToolFingerprint === fingerprint) {
        return;
      }
      child.lastToolFingerprint = fingerprint;
      yield* emit({
        ...(yield* buildEventBase({
          threadId: context.session.threadId,
          turnId,
          itemId: part.callID,
          raw,
        })),
        type: "tool.progress",
        payload: {
          taskId: linkage.taskId,
          toolName: part.tool,
          toolUseId: part.callID,
          ...(part.state.status === "running" && part.state.title
            ? { summary: part.state.title }
            : {}),
        },
      });
    });

    const emitChildCompleted = Effect.fn("emitChildCompleted")(function* (
      context: OpenCodeSessionContext,
      child: OpenCodeChildContext,
      status: OpenCodeChildTerminalStatus,
      turnId: TurnId | undefined,
      raw?: unknown,
      summary?: string,
    ) {
      if (yield* Ref.get(context.stopped)) {
        return;
      }
      const linkage = openCodeChildLinkage(child);
      if (!linkage || context.abortingChildIds.has(child.id) || child.terminalStatus) {
        return;
      }
      child.terminalStatus = status;
      yield* emit({
        ...(yield* buildEventBase({
          threadId: context.session.threadId,
          turnId,
          raw,
        })),
        type: "task.completed",
        payload: {
          ...linkage,
          status,
          ...(summary ? { summary } : {}),
        },
      });
    });

    const handleChildSubscribedEvent = Effect.fn("handleChildSubscribedEvent")(function* (
      context: OpenCodeSessionContext,
      child: OpenCodeChildContext,
      event: OpenCodeSubscribedEvent,
      currentStatusType?: "busy" | "retry" | "idle",
    ) {
      if (yield* Ref.get(context.stopped)) {
        return;
      }
      if (
        !child.linked ||
        context.abortingChildIds.has(child.id) ||
        !rememberChildEventKey(child, event)
      ) {
        return;
      }
      const turnId = context.activeTurnId;
      switch (event.type) {
        case "message.updated": {
          if (
            currentStatusType !== "busy" &&
            currentStatusType !== "retry" &&
            event.properties.info.role === "assistant" &&
            event.properties.info.error
          ) {
            yield* emitChildCompleted(
              context,
              child,
              "failed",
              turnId,
              event,
              sessionErrorMessage(event.properties.info.error),
            );
          }
          break;
        }
        case "message.part.updated": {
          if (event.properties.part.type === "tool") {
            yield* emitChildToolProgress(context, child, event.properties.part, turnId, event);
          }
          break;
        }
        case "permission.asked":
          yield* emitChildStatus(context, child, "waiting", turnId, event);
          context.pendingPermissions.set(event.properties.id, event.properties);
          yield* emit({
            ...(yield* buildEventBase({
              threadId: context.session.threadId,
              turnId,
              requestId: event.properties.id,
              raw: event,
            })),
            type: "request.opened",
            payload: {
              requestType: mapPermissionToRequestType(event.properties.permission),
              detail:
                event.properties.patterns.length > 0
                  ? event.properties.patterns.join("\n")
                  : event.properties.permission,
              args: event.properties.metadata,
            },
          });
          break;
        case "permission.replied":
          context.pendingPermissions.delete(event.properties.requestID);
          yield* emit({
            ...(yield* buildEventBase({
              threadId: context.session.threadId,
              turnId,
              requestId: event.properties.requestID,
              raw: event,
            })),
            type: "request.resolved",
            payload: {
              requestType: "unknown",
              decision: mapPermissionDecision(event.properties.reply),
            },
          });
          break;
        case "question.asked":
          yield* emitChildStatus(context, child, "waiting", turnId, event);
          context.pendingQuestions.set(event.properties.id, event.properties);
          yield* emit({
            ...(yield* buildEventBase({
              threadId: context.session.threadId,
              turnId,
              requestId: event.properties.id,
              raw: event,
            })),
            type: "user-input.requested",
            payload: {
              questions: normalizeQuestionRequest(event.properties),
            },
          });
          break;
        case "question.replied": {
          const request = context.pendingQuestions.get(event.properties.requestID);
          context.pendingQuestions.delete(event.properties.requestID);
          const answers = Object.fromEntries(
            (request?.questions ?? []).map((question, index) => [
              openCodeQuestionId(index, question),
              event.properties.answers[index]?.join(", ") ?? "",
            ]),
          );
          yield* emit({
            ...(yield* buildEventBase({
              threadId: context.session.threadId,
              turnId,
              requestId: event.properties.requestID,
              raw: event,
            })),
            type: "user-input.resolved",
            payload: { answers },
          });
          break;
        }
        case "session.status": {
          const status = openCodeProviderStatusType(event.properties.status);
          if (status === "busy") {
            yield* emitChildStatus(context, child, "running", turnId, event);
          } else if (status === "retry") {
            yield* emitChildStatus(context, child, "waiting", turnId, event);
          } else if (status === "idle") {
            yield* emitChildStatus(context, child, "idle", turnId, event);
          }
          break;
        }
        case "session.idle":
          yield* emitChildStatus(context, child, "idle", turnId, event);
          break;
        case "session.error":
          if (currentStatusType !== "busy" && currentStatusType !== "retry") {
            yield* emitChildCompleted(
              context,
              child,
              "failed",
              turnId,
              event,
              sessionErrorMessage(event.properties.error),
            );
          }
          break;
        default:
          break;
      }
    });

    const loadChildStatusMap = Effect.fn("loadOpenCodeChildStatusMap")(function* (
      context: OpenCodeSessionContext,
      childId: string,
    ) {
      if (context.childStatusMapLoaded) {
        if (
          context.childStatusMap === undefined ||
          Object.prototype.hasOwnProperty.call(context.childStatusMap, childId)
        ) {
          return context.childStatusMap;
        }
        context.childStatusMap = undefined;
        context.childStatusMapLoaded = false;
      }
      const statusMap = yield* runOpenCodeSdk("session.status", () =>
        context.client.session.status({ directory: context.directory }),
      ).pipe(
        Effect.map((response) => openCodeRecord(response.data)),
        Effect.catch((cause: OpenCodeRuntimeError) =>
          Effect.logWarning(
            `OpenCode child hydration failed for session.status (${childId}): ${openCodeRuntimeErrorDetail(cause)}`,
          ).pipe(Effect.as(undefined)),
        ),
      );
      if (statusMap !== undefined) {
        context.childStatusMap = statusMap;
        context.childStatusMapLoaded = true;
      }
      return statusMap;
    });

    const hydrateChild = Effect.fn("hydrateOpenCodeChild")(function* (
      context: OpenCodeSessionContext,
      child: OpenCodeChildContext,
      turnId: TurnId | undefined,
      hydrateToolProgress: boolean,
    ) {
      const messages = yield* runOpenCodeSdk("session.messages", () =>
        context.client.session.messages({
          sessionID: child.id,
        }),
      ).pipe(
        Effect.map((response) => response.data ?? []),
        Effect.catch((cause: OpenCodeRuntimeError) =>
          Effect.logWarning(
            `OpenCode child hydration failed for session.messages (${child.id}): ${openCodeRuntimeErrorDetail(cause)}`,
          ).pipe(Effect.as(undefined)),
        ),
      );

      let latestToolPart: Extract<Part, { type: "tool" }> | undefined;
      if (messages !== undefined) {
        for (const entry of messages) {
          for (const part of entry.parts) {
            if (part.type === "tool") {
              latestToolPart = part;
            }
          }
        }
      }
      const statusMap = yield* loadChildStatusMap(context, child.id);
      let statusType: "busy" | "retry" | "idle" | undefined;
      if (statusMap) {
        const providerStatus = statusMap[child.id];
        statusType =
          providerStatus === undefined ? "idle" : openCodeProviderStatusType(providerStatus);
      }

      const bufferedEvents = child.bufferedEvents.splice(0);
      child.bufferedEventKeys.clear();
      for (const event of bufferedEvents) {
        yield* handleChildSubscribedEvent(context, child, event, statusType);
      }

      if (statusType === "busy") {
        yield* emitChildStatus(context, child, "running", turnId);
      } else if (statusType === "retry") {
        yield* emitChildStatus(context, child, "waiting", turnId);
      } else if (statusType === "idle") {
        yield* emitChildStatus(context, child, "idle", turnId);
      }
      if (
        hydrateToolProgress &&
        latestToolPart &&
        statusType === "busy" &&
        (latestToolPart.state.status === "pending" || latestToolPart.state.status === "running")
      ) {
        yield* emitChildToolProgress(context, child, latestToolPart, turnId);
      }
    });

    const linkOpenCodeChild = Effect.fn("linkOpenCodeChild")(function* (
      context: OpenCodeSessionContext,
      part: Extract<Part, { type: "tool" }>,
      link: OpenCodeTaskLink,
      turnId: TurnId | undefined,
      raw: unknown,
    ) {
      if (
        (yield* Ref.get(context.stopped)) ||
        link.parentSessionId !== context.openCodeSessionId
      ) {
        return;
      }
      let child = context.children.get(link.childId);
      if (child && child.parentSessionId !== context.openCodeSessionId) {
        return;
      }
      if (!child) {
        child = {
          id: link.childId,
          parentSessionId: link.parentSessionId,
          description: undefined,
          role: undefined,
          model: undefined,
          background: false,
          linked: false,
          lastTaskPartId: undefined,
          lastStatus: undefined,
          terminalStatus: undefined,
          lastToolFingerprint: undefined,
          bufferedEvents: [],
          bufferedEventKeys: new Set(),
          seenEventKeys: new Set(),
        };
        context.children.set(link.childId, child);
      }

      const isNewLink = !child.linked;
      const isNewActivation = child.linked && child.lastTaskPartId !== part.id;
      if (isNewActivation) {
        context.childStatusMap = undefined;
        context.childStatusMapLoaded = false;
      }
      child.description = link.description;
      child.role = link.role;
      child.model = link.model;
      child.background = link.background;
      child.linked = true;
      child.lastTaskPartId = part.id;

      if (isNewActivation) {
        child.terminalStatus = undefined;
        child.lastStatus = undefined;
        child.lastToolFingerprint = undefined;
        child.seenEventKeys.clear();
        yield* emitChildStatus(context, child, "running", turnId, raw);
      } else if (isNewLink) {
        yield* emitChildStarted(context, child, turnId, raw);
      }

      if (!context.abortingChildIds.has(child.id)) {
        if (part.state.status === "error") {
          yield* emitChildCompleted(context, child, "failed", turnId, raw, part.state.error);
        } else if (part.state.status === "completed" && !child.background) {
          yield* emitChildCompleted(context, child, "completed", turnId, raw, part.state.output);
        }
      }

      if ((isNewLink || isNewActivation) && !(yield* Ref.get(context.stopped))) {
        yield* hydrateChild(context, child, turnId, !isNewActivation);
      }
    });

    const handleRootSubscribedEvent = Effect.fn("handleRootSubscribedEvent")(function* (
      context: OpenCodeSessionContext,
      event: OpenCodeSubscribedEvent,
    ) {
      const turnId = context.activeTurnId;
      switch (event.type) {
        case "session.updated": {
          const title = openCodeEventSessionTitle(event);
          if (title) {
            yield* emit({
              ...(yield* buildEventBase({
                threadId: context.session.threadId,
                raw: event,
              })),
              type: "thread.metadata.updated",
              payload: {
                name: title,
                metadata: {
                  sessionID: context.openCodeSessionId,
                },
              },
            });
          }
          break;
        }

        case "message.updated": {
          context.messageRoleById.set(event.properties.info.id, event.properties.info.role);
          if (event.properties.info.role === "assistant") {
            for (const part of context.partById.values()) {
              if (part.messageID !== event.properties.info.id) {
                continue;
              }
              yield* emitAssistantTextDelta(context, part, turnId, event);
            }
          }
          break;
        }

        case "message.removed": {
          context.messageRoleById.delete(event.properties.messageID);
          break;
        }

        case "message.part.delta": {
          const existingPart = context.partById.get(event.properties.partID);
          if (!existingPart) {
            break;
          }
          const role = messageRoleForPart(context, existingPart);
          if (role !== "assistant") {
            break;
          }
          const streamKind = resolveTextStreamKind(existingPart);
          const delta = event.properties.delta;
          if (delta.length === 0) {
            break;
          }
          const previousText =
            context.emittedTextByPartId.get(event.properties.partID) ??
            textFromPart(existingPart) ??
            "";
          const { nextText, deltaToEmit } = appendOpenCodeAssistantTextDelta(previousText, delta);
          if (deltaToEmit.length === 0) {
            break;
          }
          context.emittedTextByPartId.set(event.properties.partID, nextText);
          if (existingPart.type === "text" || existingPart.type === "reasoning") {
            context.partById.set(event.properties.partID, {
              ...existingPart,
              text: nextText,
            });
          }
          yield* emit({
            ...(yield* buildEventBase({
              threadId: context.session.threadId,
              turnId,
              itemId: event.properties.partID,
              raw: event,
            })),
            type: "content.delta",
            payload: {
              streamKind,
              delta: deltaToEmit,
            },
          });
          break;
        }

        case "message.part.updated": {
          const part = event.properties.part;
          context.partById.set(part.id, part);
          const messageRole = messageRoleForPart(context, part);

          if (messageRole === "assistant") {
            yield* emitAssistantTextDelta(context, part, turnId, event);
          }

          if (part.type === "tool") {
            const taskLink = part.tool === "task" ? openCodeTaskLinkFromPart(part) : undefined;
            if (taskLink) {
              yield* linkOpenCodeChild(context, part, taskLink, turnId, event);
            } else if (part.tool === "task") {
              const linkedChild = [...context.children.values()].find(
                (child) => child.linked && child.lastTaskPartId === part.id,
              );
              if (linkedChild && !context.abortingChildIds.has(linkedChild.id)) {
                if (part.state.status === "error") {
                  yield* emitChildCompleted(
                    context,
                    linkedChild,
                    "failed",
                    turnId,
                    event,
                    part.state.error,
                  );
                } else if (part.state.status === "completed" && !linkedChild.background) {
                  yield* emitChildCompleted(
                    context,
                    linkedChild,
                    "completed",
                    turnId,
                    event,
                    part.state.output,
                  );
                }
              }
            }
            const itemType = toToolLifecycleItemType(part.tool);
            const title =
              part.state.status === "running" ? (part.state.title ?? part.tool) : part.tool;
            const detail = detailFromToolPart(part);
            const payload = {
              itemType,
              ...(part.state.status === "error"
                ? { status: "failed" as const }
                : part.state.status === "completed"
                  ? { status: "completed" as const }
                  : { status: "inProgress" as const }),
              ...(title ? { title } : {}),
              ...(detail ? { detail } : {}),
              data: {
                tool: part.tool,
                state: part.state,
              },
            };
            const runtimeEvent: ProviderRuntimeEvent = {
              ...(yield* buildEventBase({
                threadId: context.session.threadId,
                turnId,
                itemId: part.callID,
                createdAt: toolStateCreatedAt(part),
                raw: event,
              })),
              type:
                part.state.status === "pending"
                  ? "item.started"
                  : part.state.status === "completed" || part.state.status === "error"
                    ? "item.completed"
                    : "item.updated",
              payload,
            };
            appendTurnItem(context, turnId, part);
            yield* emit(runtimeEvent);
          }
          break;
        }

        case "permission.asked": {
          context.pendingPermissions.set(event.properties.id, event.properties);
          yield* emit({
            ...(yield* buildEventBase({
              threadId: context.session.threadId,
              turnId,
              requestId: event.properties.id,
              raw: event,
            })),
            type: "request.opened",
            payload: {
              requestType: mapPermissionToRequestType(event.properties.permission),
              detail:
                event.properties.patterns.length > 0
                  ? event.properties.patterns.join("\n")
                  : event.properties.permission,
              args: event.properties.metadata,
            },
          });
          break;
        }

        case "permission.replied": {
          context.pendingPermissions.delete(event.properties.requestID);
          yield* emit({
            ...(yield* buildEventBase({
              threadId: context.session.threadId,
              turnId,
              requestId: event.properties.requestID,
              raw: event,
            })),
            type: "request.resolved",
            payload: {
              requestType: "unknown",
              decision: mapPermissionDecision(event.properties.reply),
            },
          });
          break;
        }

        case "question.asked": {
          context.pendingQuestions.set(event.properties.id, event.properties);
          yield* emit({
            ...(yield* buildEventBase({
              threadId: context.session.threadId,
              turnId,
              requestId: event.properties.id,
              raw: event,
            })),
            type: "user-input.requested",
            payload: {
              questions: normalizeQuestionRequest(event.properties),
            },
          });
          break;
        }

        case "question.replied": {
          const request = context.pendingQuestions.get(event.properties.requestID);
          context.pendingQuestions.delete(event.properties.requestID);
          const answers = Object.fromEntries(
            (request?.questions ?? []).map((question, index) => [
              openCodeQuestionId(index, question),
              event.properties.answers[index]?.join(", ") ?? "",
            ]),
          );
          yield* emit({
            ...(yield* buildEventBase({
              threadId: context.session.threadId,
              turnId,
              requestId: event.properties.requestID,
              raw: event,
            })),
            type: "user-input.resolved",
            payload: { answers },
          });
          break;
        }

        case "question.rejected": {
          context.pendingQuestions.delete(event.properties.requestID);
          yield* emit({
            ...(yield* buildEventBase({
              threadId: context.session.threadId,
              turnId,
              requestId: event.properties.requestID,
              raw: event,
            })),
            type: "user-input.resolved",
            payload: { answers: {} },
          });
          break;
        }

        case "session.status": {
          if (event.properties.status.type === "busy") {
            yield* updateProviderSession(context, {
              status: "running",
              activeTurnId: turnId,
            });
          }

          if (event.properties.status.type === "retry") {
            yield* emit({
              ...(yield* buildEventBase({
                threadId: context.session.threadId,
                turnId,
                raw: event,
              })),
              type: "runtime.warning",
              payload: {
                message: event.properties.status.message,
                detail: event.properties.status,
              },
            });
            break;
          }

          if (event.properties.status.type === "idle" && turnId) {
            context.activeTurnId = undefined;
            yield* updateProviderSession(context, { status: "ready" }, { clearActiveTurnId: true });
            yield* emit({
              ...(yield* buildEventBase({
                threadId: context.session.threadId,
                turnId,
                raw: event,
              })),
              type: "turn.completed",
              payload: {
                state: "completed",
              },
            });
          }
          break;
        }

        case "session.error": {
          const message = sessionErrorMessage(event.properties.error);
          const activeTurnId = context.activeTurnId;
          context.activeTurnId = undefined;
          yield* updateProviderSession(
            context,
            {
              status: "error",
              lastError: message,
            },
            { clearActiveTurnId: true },
          );
          if (activeTurnId) {
            yield* emit({
              ...(yield* buildEventBase({
                threadId: context.session.threadId,
                turnId: activeTurnId,
                raw: event,
              })),
              type: "turn.completed",
              payload: {
                state: "failed",
                errorMessage: message,
              },
            });
          }
          yield* emit({
            ...(yield* buildEventBase({
              threadId: context.session.threadId,
              raw: event,
            })),
            type: "runtime.error",
            payload: {
              message,
              class: "provider_error",
              detail: event.properties.error,
            },
          });
          break;
        }

        default:
          break;
      }
    });

    const handleSubscribedEvent = Effect.fn("handleSubscribedEvent")(function* (
      context: OpenCodeSessionContext,
      event: OpenCodeSubscribedEvent,
    ) {
      if (yield* Ref.get(context.stopped)) {
        return;
      }
      const payloadSessionId = openCodeEventSessionId(event);
      if (!payloadSessionId) {
        return;
      }
      const turnId = context.activeTurnId;
      const writeNative = Effect.gen(function* () {
        yield* writeNativeEventBestEffort(context.session.threadId, {
          observedAt: yield* nowIso,
          event: {
            provider: PROVIDER,
            threadId: context.session.threadId,
            providerThreadId: payloadSessionId,
            type: event.type,
            ...(turnId ? { turnId } : {}),
            payload: event,
          },
        });
      });

      if (payloadSessionId === context.openCodeSessionId) {
        yield* writeNative;
        yield* handleRootSubscribedEvent(context, event);
        return;
      }

      let child = context.children.get(payloadSessionId);
      if (!child && event.type === "session.created") {
        const parentSessionId = openCodeString(event.properties.info.parentID);
        if (parentSessionId !== context.openCodeSessionId) {
          return;
        }
        child = {
          id: payloadSessionId,
          parentSessionId,
          description: undefined,
          role: undefined,
          model: undefined,
          background: false,
          linked: false,
          lastTaskPartId: undefined,
          lastStatus: undefined,
          terminalStatus: undefined,
          lastToolFingerprint: undefined,
          bufferedEvents: [],
          bufferedEventKeys: new Set(),
          seenEventKeys: new Set(),
        };
        context.children.set(payloadSessionId, child);
        context.childStatusMap = undefined;
        context.childStatusMapLoaded = false;
        yield* writeNative;
        return;
      }
      if (!child) {
        return;
      }

      yield* writeNative;
      if (!child.linked) {
        bufferChildEvent(child, event);
        return;
      }
      yield* handleChildSubscribedEvent(context, child, event);
    });

    const startEventPump = Effect.fn("startEventPump")(function* (context: OpenCodeSessionContext) {
      // One AbortController per session scope. The finalizer fires when
      // the scope closes (explicit stop, unexpected exit, or layer
      // shutdown) and cancels the in-flight `event.subscribe` fetch so
      // the async iterable unwinds cleanly.
      const eventsAbortController = new AbortController();
      yield* Scope.addFinalizer(
        context.sessionScope,
        Effect.sync(() => eventsAbortController.abort()),
      );

      // Fibers forked into `context.sessionScope` are interrupted
      // automatically when the scope closes — no bookkeeping required.
      yield* Effect.flatMap(
        runOpenCodeSdk("event.subscribe", () =>
          context.client.event.subscribe(undefined, {
            signal: eventsAbortController.signal,
          }),
        ),
        (subscription) =>
          Stream.fromAsyncIterable(
            subscription.stream,
            (cause) =>
              new OpenCodeRuntimeError({
                operation: "event.subscribe",
                detail: openCodeRuntimeErrorDetail(cause),
                cause,
              }),
          ).pipe(Stream.runForEach((event) => handleSubscribedEvent(context, event))),
      ).pipe(
        Effect.exit,
        Effect.flatMap((exit) =>
          Effect.gen(function* () {
            // Expected paths: caller aborted the fetch or the session
            // has already been marked stopped. Treat as a clean exit.
            if (eventsAbortController.signal.aborted || (yield* Ref.get(context.stopped))) {
              return;
            }
            if (Exit.isFailure(exit)) {
              yield* emitUnexpectedExit(
                context,
                openCodeRuntimeErrorDetail(Cause.squash(exit.cause)),
              );
            }
          }),
        ),
        Effect.forkIn(context.sessionScope),
      );

      if (!context.server.external && context.server.exitCode !== null) {
        yield* context.server.exitCode.pipe(
          Effect.flatMap((code) =>
            Effect.gen(function* () {
              if (yield* Ref.get(context.stopped)) {
                return;
              }
              yield* emitUnexpectedExit(context, `OpenCode server exited unexpectedly (${code}).`);
            }),
          ),
          Effect.forkIn(context.sessionScope),
        );
      }
    });

    const startSession: OpenCodeAdapterShape["startSession"] = Effect.fn("startSession")(
      function* (input) {
        const binaryPath = openCodeSettings.binaryPath;
        const serverUrl = openCodeSettings.serverUrl;
        const serverPassword = openCodeSettings.serverPassword;
        const directory = input.cwd ?? serverConfig.cwd;
        const resumeSessionId = parseOpenCodeResume(input.resumeCursor)?.sessionId;
        const existing = sessions.get(input.threadId);
        if (existing) {
          yield* stopOpenCodeContext(existing);
          sessions.delete(input.threadId);
        }

        const started = yield* Effect.gen(function* () {
          const sessionScope = yield* Scope.make();
          const startedExit = yield* Effect.exit(
            Effect.gen(function* () {
              // The runtime binds the server's lifetime to the Scope.Scope
              // we provide below — closing `sessionScope` kills the child
              // process automatically. No manual `server.close()` needed.
              const server = yield* openCodeRuntime.connectToOpenCodeServer({
                binaryPath,
                serverUrl,
                ...(options?.environment ? { environment: options.environment } : {}),
              });
              const client = openCodeRuntime.createOpenCodeSdkClient({
                baseUrl: server.url,
                directory,
                ...(server.external && serverPassword ? { serverPassword } : {}),
              });
              const mcpSession = McpProviderSession.readMcpProviderSession(input.threadId);
              if (mcpSession && !server.external) {
                yield* runOpenCodeSdk("mcp.add", () =>
                  client.mcp.add({
                    name: "t3-code",
                    config: {
                      type: "remote",
                      url: mcpSession.endpoint,
                      headers: {
                        Authorization: mcpSession.authorizationHeader,
                      },
                      oauth: false,
                    },
                  }),
                );
              }
              // Resume: re-adopt the session named by the durable cursor —
              // OpenCode scopes history by session id. The probe recovers only
              // a confirmed not-found (start fresh); transport/auth/server
              // errors propagate instead of masking as a new empty session.
              const resolved = yield* Effect.gen(function* () {
                const adopted = resumeSessionId
                  ? yield* runOpenCodeSdk("session.get", () =>
                      client.session.get({ sessionID: resumeSessionId }),
                    ).pipe(
                      Effect.map((response) => response.data),
                      Effect.catchIf(
                        (cause) => isOpenCodeNotFound(cause),
                        () => Effect.void,
                      ),
                    )
                  : undefined;

                // Reuse in place only when the session still matches the
                // requested cwd; on a cwd change it is forked below instead.
                const reusable =
                  adopted &&
                  (!adopted.directory || (yield* sameDirectory(adopted.directory, directory)))
                    ? adopted
                    : undefined;

                if (reusable) {
                  // Resume skips `session.create`, so re-assert the ruleset —
                  // a runtime-mode change would otherwise leave the session on
                  // its original permissions.
                  yield* runOpenCodeSdk("session.update", () =>
                    client.session.update({
                      sessionID: reusable.id,
                      permission: buildOpenCodePermissionRules(input.runtimeMode),
                    }),
                  );
                  return { openCodeSession: reusable, created: false };
                }

                // The session lives under a different cwd (e.g. the thread
                // moved into a git worktree). Fork it into the requested
                // directory instead of minting an empty one — the fork carries
                // the full history, so the follow-up keeps its context (#3604).
                if (adopted) {
                  yield* Effect.logInfo(
                    `OpenCode session '${adopted.id}' was created under a different working directory; forking into '${directory}' to preserve conversation history.`,
                  );
                  const forkedSession = yield* runOpenCodeSdk("session.fork", () =>
                    client.session.fork({ sessionID: adopted.id, directory }),
                  );
                  const forked = forkedSession.data;
                  if (!forked) {
                    return yield* new OpenCodeRuntimeError({
                      operation: "session.fork",
                      detail: "OpenCode session.fork returned no session payload.",
                    });
                  }
                  yield* runOpenCodeSdk("session.update", () =>
                    client.session.update({
                      sessionID: forked.id,
                      permission: buildOpenCodePermissionRules(input.runtimeMode),
                    }),
                  );
                  return { openCodeSession: forked, created: true };
                }

                if (resumeSessionId) {
                  yield* Effect.logWarning(
                    `OpenCode session '${resumeSessionId}' no longer exists; starting a fresh session.`,
                  );
                }
                const createdSession = yield* runOpenCodeSdk("session.create", () =>
                  client.session.create({
                    permission: buildOpenCodePermissionRules(input.runtimeMode),
                  }),
                );
                if (!createdSession.data) {
                  return yield* new OpenCodeRuntimeError({
                    operation: "session.create",
                    detail: "OpenCode session.create returned no session payload.",
                  });
                }
                return { openCodeSession: createdSession.data, created: true };
              });

              return {
                sessionScope,
                server,
                client,
                openCodeSession: resolved.openCodeSession,
                created: resolved.created,
              };
            }).pipe(Effect.provideService(Scope.Scope, sessionScope)),
          );
          if (Exit.isFailure(startedExit)) {
            yield* Scope.close(sessionScope, Exit.void).pipe(Effect.ignore);
            return yield* toProcessError(input.threadId, Cause.squash(startedExit.cause));
          }
          return startedExit.value;
        });

        // Guard against a concurrent startSession call that may have raced
        // and already inserted a session while we were awaiting async work.
        const raceWinner = sessions.get(input.threadId);
        if (raceWinner) {
          // Another call won the race — clean up. Only abort the remote
          // session if we created it here; a resumed one is shared upstream
          // state the winner is now using.
          if (started.created) {
            yield* runOpenCodeSdk("session.abort", () =>
              started.client.session.abort({
                sessionID: started.openCodeSession.id,
              }),
            ).pipe(Effect.ignore);
          }
          yield* Scope.close(started.sessionScope, Exit.void).pipe(Effect.ignore);
          return raceWinner.session;
        }

        const createdAt = yield* nowIso;
        const session: ProviderSession = {
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          status: "ready",
          runtimeMode: input.runtimeMode,
          cwd: directory,
          ...(input.modelSelection ? { model: input.modelSelection.model } : {}),
          threadId: input.threadId,
          // ProviderService persists this cursor and feeds it back into
          // `startSession` after the in-memory session is lost (reaper /
          // restart), so follow-ups continue the same conversation (#3604).
          resumeCursor: {
            schemaVersion: OPENCODE_RESUME_VERSION,
            sessionId: started.openCodeSession.id,
          },
          createdAt,
          updatedAt: createdAt,
        };

        const emitChildStopped = (
          context: OpenCodeSessionContext,
          childId: string,
        ): Effect.Effect<void, never, never> =>
          Effect.gen(function* () {
            const child = context.children.get(childId);
            const linkage = child ? openCodeChildLinkage(child) : undefined;
            if (!child || !linkage || child.terminalStatus) {
              return;
            }
            child.terminalStatus = "stopped";
            yield* emit({
              ...(yield* buildEventBase({
                threadId: context.session.threadId,
                turnId: context.activeTurnId,
              })),
              type: "task.completed",
              payload: {
                ...linkage,
                status: "stopped",
              },
            });
          }).pipe(Effect.catchCause(() => Effect.void));

        const context: OpenCodeSessionContext = {
          session,
          client: started.client,
          server: started.server,
          directory,
          openCodeSessionId: started.openCodeSession.id,
          pendingPermissions: new Map(),
          pendingQuestions: new Map(),
          partById: new Map(),
          emittedTextByPartId: new Map(),
          messageRoleById: new Map(),
          completedAssistantPartIds: new Set(),
          children: new Map(),
          abortingChildIds: new Set(),
          childStatusMap: undefined,
          childStatusMapLoaded: false,
          emitChildStopped: () => Effect.void,
          turns: [],
          activeTurnId: undefined,
          activeAgent: undefined,
          activeVariant: undefined,
          stopped: yield* Ref.make(false),
          sessionScope: started.sessionScope,
        };
        context.emitChildStopped = (childId) => emitChildStopped(context, childId);
        sessions.set(input.threadId, context);
        yield* startEventPump(context);

        yield* emit({
          ...(yield* buildEventBase({ threadId: input.threadId })),
          type: "session.started",
          payload: {
            message: "OpenCode session started",
          },
        });
        yield* emit({
          ...(yield* buildEventBase({ threadId: input.threadId })),
          type: "thread.started",
          payload: {
            providerThreadId: started.openCodeSession.id,
          },
        });

        return session;
      },
    );

    const sendTurn: OpenCodeAdapterShape["sendTurn"] = Effect.fn("sendTurn")(function* (input) {
      const context = yield* ensureSessionContext(sessions, input.threadId);
      // A sendTurn while a turn is active is a steer: OpenCode queues the
      // prompt into the busy session and the work continues as one turn, so
      // the active turn id is reused instead of opening a new turn.
      const steeringTurnId = context.activeTurnId;
      const turnId = steeringTurnId ?? TurnId.make(`opencode-turn-${yield* randomUUIDv4}`);
      const modelSelection =
        input.modelSelection ??
        (context.session.model
          ? { instanceId: boundInstanceId, model: context.session.model }
          : undefined);
      if (modelSelection !== undefined && modelSelection.instanceId !== boundInstanceId) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: `OpenCode model selection is bound to instance '${modelSelection?.instanceId}', expected '${boundInstanceId}'.`,
        });
      }
      const parsedModel = parseOpenCodeModelSlug(modelSelection?.model);
      if (!parsedModel) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: "OpenCode model selection must use the 'provider/model' format.",
        });
      }

      const text = input.input?.trim();
      const fileParts = toOpenCodeFileParts({
        attachments: input.attachments,
        resolveAttachmentPath: (attachment) =>
          resolveAttachmentPath({
            attachmentsDir: serverConfig.attachmentsDir,
            attachment,
          }),
      });
      if ((!text || text.length === 0) && fileParts.length === 0) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: "OpenCode turns require text input or at least one attachment.",
        });
      }

      const agent = getModelSelectionStringOptionValue(modelSelection, "agent");
      const variant = getModelSelectionStringOptionValue(modelSelection, "variant");

      context.activeTurnId = turnId;
      context.activeAgent = agent ?? (input.interactionMode === "plan" ? "plan" : undefined);
      context.activeVariant = variant;
      yield* updateProviderSession(
        context,
        {
          status: "running",
          activeTurnId: turnId,
          model: modelSelection?.model ?? context.session.model,
        },
        { clearLastError: true },
      );

      if (steeringTurnId === undefined) {
        yield* emit({
          ...(yield* buildEventBase({ threadId: input.threadId, turnId })),
          type: "turn.started",
          payload: {
            model: modelSelection?.model ?? context.session.model,
            ...(variant ? { effort: variant } : {}),
          },
        });
      }

      yield* runOpenCodeSdk("session.promptAsync", () =>
        context.client.session.promptAsync({
          sessionID: context.openCodeSessionId,
          model: parsedModel,
          ...(context.activeAgent ? { agent: context.activeAgent } : {}),
          ...(context.activeVariant ? { variant: context.activeVariant } : {}),
          parts: [...(text ? [{ type: "text" as const, text }] : []), ...fileParts],
        }),
      ).pipe(
        Effect.mapError(toRequestError),
        // On failure of a fresh turn: clear active-turn state, flip the
        // session back to ready with lastError set, emit turn.aborted, then
        // let the typed error propagate. We don't need to rebuild the error
        // here — `toRequestError` already produced the right shape. A failed
        // steer leaves the still-running original turn untouched.
        Effect.tapError((requestError) =>
          steeringTurnId !== undefined
            ? Effect.void
            : Effect.gen(function* () {
                context.activeTurnId = undefined;
                context.activeAgent = undefined;
                context.activeVariant = undefined;
                yield* updateProviderSession(
                  context,
                  {
                    status: "ready",
                    model: modelSelection?.model ?? context.session.model,
                    lastError: requestError.detail,
                  },
                  { clearActiveTurnId: true },
                );
                yield* emit({
                  ...(yield* buildEventBase({
                    threadId: input.threadId,
                    turnId,
                  })),
                  type: "turn.aborted",
                  payload: {
                    reason: requestError.detail,
                  },
                });
              }),
        ),
      );

      return {
        threadId: input.threadId,
        turnId,
        // Re-surface the durable cursor on every turn so the persisted binding
        // is refreshed alongside last-seen/runtime state (mirrors Grok/Codex).
        ...(context.session.resumeCursor !== undefined
          ? { resumeCursor: context.session.resumeCursor }
          : {}),
      };
    });

    const interruptTurn: OpenCodeAdapterShape["interruptTurn"] = Effect.fn("interruptTurn")(
      function* (threadId, turnId) {
        const context = yield* ensureSessionContext(sessions, threadId);
        const stoppedChildIds = yield* abortTrackedOpenCodeChildren(context);
        yield* Effect.forEach(
          stoppedChildIds,
          (childId) => context.emitChildStopped(childId).pipe(Effect.ignore),
          { concurrency: "unbounded", discard: true },
        );
        clearAbortingOpenCodeChildren(context, stoppedChildIds);
        yield* runOpenCodeSdk("session.abort", () =>
          context.client.session.abort({ sessionID: context.openCodeSessionId }),
        ).pipe(Effect.mapError(toRequestError));
        if (turnId ?? context.activeTurnId) {
          yield* emit({
            ...(yield* buildEventBase({
              threadId,
              turnId: turnId ?? context.activeTurnId,
            })),
            type: "turn.aborted",
            payload: {
              reason: "Interrupted by user.",
            },
          });
        }
      },
    );

    const respondToRequest: OpenCodeAdapterShape["respondToRequest"] = Effect.fn(
      "respondToRequest",
    )(function* (threadId, requestId, decision) {
      const context = yield* ensureSessionContext(sessions, threadId);
      if (!context.pendingPermissions.has(requestId)) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "permission.reply",
          detail: `Unknown pending permission request: ${requestId}`,
        });
      }

      yield* runOpenCodeSdk("permission.reply", () =>
        context.client.permission.reply({
          requestID: requestId,
          reply: toOpenCodePermissionReply(decision),
        }),
      ).pipe(Effect.mapError(toRequestError));
    });

    const respondToUserInput: OpenCodeAdapterShape["respondToUserInput"] = Effect.fn(
      "respondToUserInput",
    )(function* (threadId, requestId, answers) {
      const context = yield* ensureSessionContext(sessions, threadId);
      const request = context.pendingQuestions.get(requestId);
      if (!request) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "question.reply",
          detail: `Unknown pending user-input request: ${requestId}`,
        });
      }

      yield* runOpenCodeSdk("question.reply", () =>
        context.client.question.reply({
          requestID: requestId,
          answers: toOpenCodeQuestionAnswers(request, answers),
        }),
      ).pipe(Effect.mapError(toRequestError));
    });

    const stopSession: OpenCodeAdapterShape["stopSession"] = Effect.fn("stopSession")(
      function* (threadId) {
        const context = sessions.get(threadId);
        if (!context) {
          return yield* new ProviderAdapterSessionNotFoundError({
            provider: PROVIDER,
            threadId,
          });
        }
        const stopped = yield* stopOpenCodeContext(context);
        sessions.delete(threadId);
        if (!stopped) {
          return;
        }
        yield* emit({
          ...(yield* buildEventBase({ threadId })),
          type: "session.exited",
          payload: {
            reason: "Session stopped.",
            recoverable: false,
            exitKind: "graceful",
          },
        });
      },
    );

    const listSessions: OpenCodeAdapterShape["listSessions"] = () =>
      Effect.sync(() => [...sessions.values()].map((context) => context.session));

    const hasSession: OpenCodeAdapterShape["hasSession"] = (threadId) =>
      Effect.sync(() => sessions.has(threadId));

    const readThread: OpenCodeAdapterShape["readThread"] = Effect.fn("readThread")(
      function* (threadId) {
        const context = yield* ensureSessionContext(sessions, threadId);
        const messages = yield* runOpenCodeSdk("session.messages", () =>
          context.client.session.messages({
            sessionID: context.openCodeSessionId,
          }),
        ).pipe(Effect.mapError(toRequestError));

        const turns: Array<OpenCodeTurnSnapshot> = [];
        for (const entry of messages.data ?? []) {
          if (entry.info.role === "assistant") {
            turns.push({
              id: TurnId.make(entry.info.id),
              items: [entry.info, ...entry.parts],
            });
          }
        }

        return {
          threadId,
          turns,
        };
      },
    );

    const rollbackThread: OpenCodeAdapterShape["rollbackThread"] = Effect.fn("rollbackThread")(
      function* (threadId, numTurns) {
        const context = yield* ensureSessionContext(sessions, threadId);
        const messages = yield* runOpenCodeSdk("session.messages", () =>
          context.client.session.messages({
            sessionID: context.openCodeSessionId,
          }),
        ).pipe(Effect.mapError(toRequestError));

        const assistantMessages = (messages.data ?? []).filter(
          (entry) => entry.info.role === "assistant",
        );
        const targetIndex = assistantMessages.length - numTurns - 1;
        const target = targetIndex >= 0 ? assistantMessages[targetIndex] : null;
        yield* runOpenCodeSdk("session.revert", () =>
          context.client.session.revert({
            sessionID: context.openCodeSessionId,
            ...(target ? { messageID: target.info.id } : {}),
          }),
        ).pipe(Effect.mapError(toRequestError));

        return yield* readThread(threadId);
      },
    );

    const stopAll: OpenCodeAdapterShape["stopAll"] = () =>
      Effect.gen(function* () {
        const contexts = [...sessions.values()];
        sessions.clear();
        // `stopOpenCodeContext` is typed as never-failing — SDK aborts are
        // already `Effect.ignore`'d inside it. `ignoreCause` here also
        // swallows defects from throwing finalizers so one bad close can't
        // interrupt the sibling fibers. Same pattern as the layer finalizer.
        yield* Effect.forEach(
          contexts,
          (context) => Effect.ignoreCause(stopOpenCodeContext(context)),
          { concurrency: "unbounded", discard: true },
        );
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
      get streamEvents() {
        return Stream.fromQueue(runtimeEvents);
      },
    } satisfies OpenCodeAdapterShape;
  });
}
