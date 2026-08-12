// @effect-diagnostics nodeBuiltinImport:off - Tests exercise the Unix socket security boundary.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { Readable } from "node:stream";

import { afterEach, assert, describe, expect, it } from "vitest";

import {
  EPHEMERAL_PROVIDER_ENV_MAX_VALUE_BYTES,
  EphemeralProviderEnvIpcError,
  makeEphemeralProviderEnvRequest,
  sendEphemeralProviderEnvRequest,
  startEphemeralProviderEnvIpcServer,
} from "./ephemeralProviderEnv.ts";
import { readEphemeralProviderEnvValue } from "../cli/providerEnv.ts";

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  while (cleanups.length > 0) await cleanups.pop()?.();
});

describe.skipIf(process.platform === "win32")("ephemeral provider environment IPC", () => {
  it("loads and clears a value through an owner-only socket", async () => {
    const stateDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-env-"));
    const mutations: Array<{ operation: string; value?: string }> = [];
    const server = await startEphemeralProviderEnvIpcServer({
      stateDir,
      handler: {
        hasProviderInstance: (instanceId) => instanceId === "codex_work",
        load: ({ value }) => mutations.push({ operation: "load", value }),
        clear: () => mutations.push({ operation: "clear" }),
      },
    });
    cleanups.push(server.close);

    assert.equal(NodeFS.statSync(server.socketPath).mode & 0o777, 0o600);
    await sendEphemeralProviderEnvRequest({
      socketPath: server.socketPath,
      request: makeEphemeralProviderEnvRequest({
        operation: "load",
        instanceId: "codex_work",
        value: "stdin-only-secret",
      }),
    });
    await sendEphemeralProviderEnvRequest({
      socketPath: server.socketPath,
      request: makeEphemeralProviderEnvRequest({
        operation: "clear",
        instanceId: "codex_work",
      }),
    });

    assert.deepEqual(mutations, [
      { operation: "load", value: "stdin-only-secret" },
      { operation: "clear" },
    ]);
  });

  it("validates identifiers, empty input, and input size", () => {
    const invalid = [
      { operation: "load" as const, instanceId: "bad instance", value: "x" },
      { operation: "load" as const, instanceId: "codex", value: "" },
      {
        operation: "load" as const,
        instanceId: "codex",
        value: "x".repeat(EPHEMERAL_PROVIDER_ENV_MAX_VALUE_BYTES + 1),
      },
    ];
    for (const input of invalid) {
      assert.throws(() => makeEphemeralProviderEnvRequest(input), EphemeralProviderEnvIpcError);
    }
  });

  it("reads the value from stdin and removes one shell line ending", async () => {
    assert.equal(
      await readEphemeralProviderEnvValue(Readable.from(["stdin-only-secret\n"])),
      "stdin-only-secret",
    );
    await expect(readEphemeralProviderEnvValue(Readable.from(["\n"]))).rejects.toBeInstanceOf(
      EphemeralProviderEnvIpcError,
    );
  });

  it("rejects unknown instances without returning the secret", async () => {
    const stateDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-env-"));
    const server = await startEphemeralProviderEnvIpcServer({
      stateDir,
      handler: {
        hasProviderInstance: () => false,
        load: () => assert.fail("load must not run"),
        clear: () => assert.fail("clear must not run"),
      },
    });
    cleanups.push(server.close);

    const secret = "must-not-appear";
    await expect(
      sendEphemeralProviderEnvRequest({
        socketPath: server.socketPath,
        request: makeEphemeralProviderEnvRequest({
          operation: "load",
          instanceId: "missing",
          value: secret,
        }),
      }),
    ).rejects.toMatchObject({
      code: "provider_instance_not_found",
      message: expect.not.stringContaining(secret),
    });
  });

  it("refuses a socket with group or other permissions", async () => {
    const stateDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-env-"));
    const server = await startEphemeralProviderEnvIpcServer({
      stateDir,
      handler: {
        hasProviderInstance: () => true,
        load: () => undefined,
        clear: () => undefined,
      },
    });
    cleanups.push(server.close);
    NodeFS.chmodSync(server.socketPath, 0o660);

    await expect(
      sendEphemeralProviderEnvRequest({
        socketPath: server.socketPath,
        request: makeEphemeralProviderEnvRequest({
          operation: "clear",
          instanceId: "codex",
        }),
      }),
    ).rejects.toMatchObject({ code: "socket_insecure" });
  });
});
