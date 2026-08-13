// @effect-diagnostics nodeBuiltinImport:off - Local IPC uses owner-only Unix sockets or Windows named pipes.
import { createHash } from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeNet from "node:net";
import * as NodePath from "node:path";

import { ProviderInstanceId, type ProviderInstanceId as ProviderId } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

export const EPHEMERAL_PROVIDER_ENV_SOCKET_NAME = "provider-env.sock";
export const EPHEMERAL_PROVIDER_ENV_MAX_VALUE_BYTES = 64 * 1024;
const WINDOWS_NAMED_PIPE_PREFIX = "\\\\.\\pipe\\";
const WINDOWS_NAMED_PIPE_NAME = /^\\\\\.\\pipe\\t3-provider-env-[a-f0-9]{32}$/;
const MAX_REQUEST_BYTES = EPHEMERAL_PROVIDER_ENV_MAX_VALUE_BYTES + 1024;

export type EphemeralProviderEnvRequest =
  | {
      readonly version: 1;
      readonly operation: "load";
      readonly instanceId: ProviderId;
      readonly value: string;
    }
  | {
      readonly version: 1;
      readonly operation: "clear";
      readonly instanceId: ProviderId;
    };

export interface EphemeralProviderEnvHandler {
  readonly hasProviderInstance: (instanceId: ProviderId) => boolean | Promise<boolean>;
  readonly load: (input: {
    readonly instanceId: ProviderId;
    readonly value: string;
  }) => void | Promise<void>;
  readonly clear: (instanceId: ProviderId) => void | Promise<void>;
}

export type EphemeralProviderEnvResponse =
  | { readonly ok: true }
  | {
      readonly ok: false;
      readonly code: "invalid_request" | "provider_instance_not_found" | "mutation_failed";
    };

export class EphemeralProviderEnvIpcError extends Error {
  constructor(
    readonly code:
      | "invalid_request"
      | "socket_unavailable"
      | "socket_insecure"
      | "provider_instance_not_found"
      | "mutation_failed",
  ) {
    super(
      {
        invalid_request: "Invalid Bitwarden session request.",
        socket_unavailable: "The local T3 server Bitwarden session endpoint is unavailable.",
        socket_insecure: "The local T3 server Bitwarden session endpoint is not trusted.",
        provider_instance_not_found: "The selected provider instance is not configured.",
        mutation_failed: "The running T3 server could not update the Bitwarden session.",
      }[code],
    );
    this.name = "EphemeralProviderEnvIpcError";
  }
}

const decodeProviderInstanceId = Schema.decodeUnknownSync(ProviderInstanceId);

function windowsNamedPipePath(stateDir: string): string {
  // The server and the CLI can receive the same Windows path with different
  // casing or slash styles. Normalize before hashing so both processes select
  // the same pipe. Keep a filesystem root's trailing separator intact.
  const normalized = NodePath.win32.normalize(stateDir).toLowerCase();
  const root = NodePath.win32.parse(normalized).root;
  const canonical = normalized === root ? normalized : normalized.replace(/[\\]+$/, "");
  const stateHash = createHash("sha256").update(canonical).digest("hex").slice(0, 32);
  return `${WINDOWS_NAMED_PIPE_PREFIX}t3-provider-env-${stateHash}`;
}

export function ephemeralProviderEnvSocketPath(
  stateDir: string,
  platform: NodeJS.Platform = process.platform,
): string {
  if (platform === "win32") return windowsNamedPipePath(stateDir);
  return NodePath.join(stateDir, EPHEMERAL_PROVIDER_ENV_SOCKET_NAME);
}

export function isEphemeralProviderEnvNamedPipePath(socketPath: string): boolean {
  return WINDOWS_NAMED_PIPE_NAME.test(socketPath);
}

export function makeEphemeralProviderEnvRequest(input: {
  readonly operation: "load" | "clear";
  readonly instanceId: string;
  readonly value?: string;
}): EphemeralProviderEnvRequest {
  try {
    const instanceId = decodeProviderInstanceId(input.instanceId);
    if (input.operation === "clear") {
      return { version: 1, operation: "clear", instanceId };
    }
    if (input.value === undefined || Buffer.byteLength(input.value) === 0) {
      throw new EphemeralProviderEnvIpcError("invalid_request");
    }
    if (Buffer.byteLength(input.value) > EPHEMERAL_PROVIDER_ENV_MAX_VALUE_BYTES) {
      throw new EphemeralProviderEnvIpcError("invalid_request");
    }
    return { version: 1, operation: "load", instanceId, value: input.value };
  } catch (cause) {
    if (cause instanceof EphemeralProviderEnvIpcError) throw cause;
    throw new EphemeralProviderEnvIpcError("invalid_request");
  }
}

function parseRequest(raw: string): EphemeralProviderEnvRequest {
  try {
    const input = JSON.parse(raw) as Record<string, unknown>;
    if (input.version !== 1 || (input.operation !== "load" && input.operation !== "clear")) {
      throw new EphemeralProviderEnvIpcError("invalid_request");
    }
    return makeEphemeralProviderEnvRequest({
      operation: input.operation,
      instanceId: typeof input.instanceId === "string" ? input.instanceId : "",
      ...(input.operation === "load" && typeof input.value === "string"
        ? { value: input.value }
        : {}),
    });
  } catch (cause) {
    if (cause instanceof EphemeralProviderEnvIpcError) throw cause;
    throw new EphemeralProviderEnvIpcError("invalid_request");
  }
}

async function handleRequest(
  raw: string,
  handler: EphemeralProviderEnvHandler,
): Promise<EphemeralProviderEnvResponse> {
  let request: EphemeralProviderEnvRequest;
  try {
    request = parseRequest(raw);
  } catch {
    return { ok: false, code: "invalid_request" };
  }
  if (!(await handler.hasProviderInstance(request.instanceId))) {
    return { ok: false, code: "provider_instance_not_found" };
  }
  try {
    if (request.operation === "load") {
      await handler.load(request);
    } else {
      await handler.clear(request.instanceId);
    }
    return { ok: true };
  } catch {
    return { ok: false, code: "mutation_failed" };
  }
}

export async function startEphemeralProviderEnvIpcServer(input: {
  readonly stateDir: string;
  readonly handler: EphemeralProviderEnvHandler;
}): Promise<{ readonly socketPath: string; readonly close: () => Promise<void> }> {
  const isWindows = process.platform === "win32";
  const socketPath = ephemeralProviderEnvSocketPath(input.stateDir);
  if (!isWindows) {
    NodeFS.mkdirSync(input.stateDir, { recursive: true, mode: 0o700 });
    try {
      const stale = NodeFS.lstatSync(socketPath);
      if (
        !stale.isSocket() ||
        (typeof process.getuid === "function" && stale.uid !== process.getuid())
      ) {
        throw new EphemeralProviderEnvIpcError("socket_insecure");
      }
      NodeFS.unlinkSync(socketPath);
    } catch (cause) {
      if (!(cause instanceof Error && "code" in cause && cause.code === "ENOENT")) throw cause;
    }
  }

  const server = NodeNet.createServer((socket) => {
    let raw = "";
    let byteCount = 0;
    let responded = false;
    let requestStarted = false;
    const respond = (response: EphemeralProviderEnvResponse) => {
      if (responded) return;
      responded = true;
      socket.end(`${JSON.stringify(response)}\n`);
    };
    socket.setEncoding("utf8");
    socket.on("data", (chunk: string) => {
      byteCount += Buffer.byteLength(chunk);
      if (byteCount > MAX_REQUEST_BYTES) {
        respond({ ok: false, code: "invalid_request" });
        return;
      }
      raw += chunk;
      const newline = raw.indexOf("\n");
      if (newline >= 0 && !requestStarted) {
        requestStarted = true;
        void handleRequest(raw.slice(0, newline), input.handler).then(respond);
      }
    });
    socket.on("end", () => {
      if (!requestStarted && raw.length > 0) {
        requestStarted = true;
        void handleRequest(raw, input.handler).then(respond);
      }
    });
    socket.on("error", () => undefined);
  });

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(socketPath, () => {
      server.off("error", reject);
      resolve();
    });
  });
  if (!isWindows) NodeFS.chmodSync(socketPath, 0o600);

  return {
    socketPath,
    close: async () => {
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
      if (!isWindows) {
        try {
          NodeFS.unlinkSync(socketPath);
        } catch (cause) {
          if (!(cause instanceof Error && "code" in cause && cause.code === "ENOENT")) throw cause;
        }
      }
    },
  };
}

function assertSecureSocket(socketPath: string): void {
  let stat: NodeFS.Stats;
  try {
    stat = NodeFS.lstatSync(socketPath);
  } catch {
    throw new EphemeralProviderEnvIpcError("socket_unavailable");
  }
  const wrongOwner = typeof process.getuid === "function" && stat.uid !== process.getuid();
  if (!stat.isSocket() || wrongOwner || (stat.mode & 0o077) !== 0) {
    throw new EphemeralProviderEnvIpcError("socket_insecure");
  }
}

function assertSecureEndpoint(socketPath: string): void {
  if (process.platform === "win32") {
    // Node/libuv creates named pipes with the current process token's default
    // DACL. Restrict the client to our derived endpoint shape so callers cannot
    // redirect a session value to an arbitrary named pipe.
    if (!isEphemeralProviderEnvNamedPipePath(socketPath)) {
      throw new EphemeralProviderEnvIpcError("socket_insecure");
    }
    return;
  }
  assertSecureSocket(socketPath);
}

export async function sendEphemeralProviderEnvRequest(input: {
  readonly socketPath: string;
  readonly request: EphemeralProviderEnvRequest;
}): Promise<void> {
  assertSecureEndpoint(input.socketPath);
  const response = await new Promise<EphemeralProviderEnvResponse>((resolve, reject) => {
    const socket = NodeNet.createConnection(input.socketPath);
    let raw = "";
    socket.setEncoding("utf8");
    socket.once("connect", () => socket.write(`${JSON.stringify(input.request)}\n`));
    socket.on("data", (chunk: string) => {
      raw += chunk;
    });
    socket.once("end", () => {
      try {
        resolve(JSON.parse(raw.trim()) as EphemeralProviderEnvResponse);
      } catch {
        reject(new EphemeralProviderEnvIpcError("socket_unavailable"));
      }
    });
    socket.once("error", () => reject(new EphemeralProviderEnvIpcError("socket_unavailable")));
  });
  if (!response.ok) throw new EphemeralProviderEnvIpcError(response.code);
}
