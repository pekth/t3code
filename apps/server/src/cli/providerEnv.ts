// @effect-diagnostics nodeBuiltinImport:off - Secrets are deliberately read directly from stdin.
import type { Readable } from "node:stream";

import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import { Argument, Command, GlobalFlag } from "effect/unstable/cli";

import {
  EPHEMERAL_PROVIDER_ENV_MAX_VALUE_BYTES,
  EphemeralProviderEnvIpcError,
  ephemeralProviderEnvSocketPath,
  makeEphemeralProviderEnvRequest,
  sendEphemeralProviderEnvRequest,
} from "../localIpc/ephemeralProviderEnv.ts";
import { authLocationFlags, type CliAuthLocationFlags, resolveCliAuthConfig } from "./config.ts";

export async function readEphemeralProviderEnvValue(
  stdin: Readable = process.stdin,
): Promise<string> {
  const chunks: Buffer[] = [];
  let bytes = 0;
  for await (const chunk of stdin) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    bytes += buffer.byteLength;
    if (bytes > EPHEMERAL_PROVIDER_ENV_MAX_VALUE_BYTES + 2) {
      throw new EphemeralProviderEnvIpcError("invalid_request");
    }
    chunks.push(buffer);
  }
  let value = Buffer.concat(chunks).toString("utf8");
  if (value.endsWith("\r\n")) value = value.slice(0, -2);
  else if (value.endsWith("\n")) value = value.slice(0, -1);
  if (
    Buffer.byteLength(value) === 0 ||
    Buffer.byteLength(value) > EPHEMERAL_PROVIDER_ENV_MAX_VALUE_BYTES
  ) {
    throw new EphemeralProviderEnvIpcError("invalid_request");
  }
  return value;
}

const runProviderEnvCommand = (
  flags: CliAuthLocationFlags & {
    readonly instanceId: string;
  },
  operation: "load" | "clear",
) =>
  Effect.gen(function* () {
    const logLevel = yield* GlobalFlag.LogLevel;
    const config = yield* resolveCliAuthConfig(flags, logLevel);
    const value =
      operation === "load"
        ? yield* Effect.tryPromise({
            try: () => readEphemeralProviderEnvValue(),
            catch: () => new EphemeralProviderEnvIpcError("invalid_request"),
          })
        : undefined;
    const request = yield* Effect.try({
      try: () =>
        makeEphemeralProviderEnvRequest({
          operation,
          instanceId: flags.instanceId,
          ...(value === undefined ? {} : { value }),
        }),
      catch: () => new EphemeralProviderEnvIpcError("invalid_request"),
    });
    yield* Effect.tryPromise({
      try: () =>
        sendEphemeralProviderEnvRequest({
          socketPath: ephemeralProviderEnvSocketPath(config.stateDir),
          request,
        }),
      catch: (cause) =>
        cause instanceof EphemeralProviderEnvIpcError
          ? cause
          : new EphemeralProviderEnvIpcError("socket_unavailable"),
    });
    yield* Console.log(
      operation === "load"
        ? "BW_SESSION loaded for the selected provider instance."
        : "BW_SESSION cleared from the selected provider instance.",
    );
  });

const providerEnvArguments = {
  ...authLocationFlags,
  instanceId: Argument.string("provider-instance").pipe(
    Argument.withDescription("Configured provider instance id."),
  ),
};

const providerEnvLoadCommand = Command.make("load", providerEnvArguments).pipe(
  Command.withDescription("Read BW_SESSION from stdin for one running provider instance."),
  Command.withHandler((flags) => runProviderEnvCommand(flags, "load")),
);

const providerEnvClearCommand = Command.make("clear", providerEnvArguments).pipe(
  Command.withDescription("Clear BW_SESSION for one running provider instance."),
  Command.withHandler((flags) => runProviderEnvCommand(flags, "clear")),
);

export const providerEnvCommand = Command.make("provider-env").pipe(
  Command.withDescription("Manage in-memory BW_SESSION values on the local server."),
  Command.withSubcommands([providerEnvLoadCommand, providerEnvClearCommand]),
);
