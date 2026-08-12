import { assert, describe, it } from "@effect/vitest";
import { ProviderInstanceId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import { ProviderEnvironmentOverlay } from "../Services/ProviderEnvironmentOverlay.ts";
import { layer } from "./ProviderEnvironmentOverlay.ts";

const instanceId = ProviderInstanceId.make("codex-work");

describe("ProviderEnvironmentOverlay", () => {
  it.effect("loads, replaces, and clears an instance-scoped BW_SESSION", () =>
    Effect.gen(function* () {
      const overlay = yield* ProviderEnvironmentOverlay;
      const configured = [
        { name: "BW_SESSION", value: "configured", sensitive: true },
        { name: "PATH", value: "/bin", sensitive: false },
      ] as const;

      yield* overlay.load({ instanceId, value: "ephemeral-1" });
      assert.deepEqual(yield* overlay.resolve(instanceId, configured), [
        { name: "BW_SESSION", value: "ephemeral-1", sensitive: true },
        { name: "PATH", value: "/bin", sensitive: false },
      ]);

      yield* overlay.load({ instanceId, value: "ephemeral-2" });
      assert.equal((yield* overlay.resolve(instanceId, configured))[0]?.value, "ephemeral-2");

      yield* overlay.clear(instanceId);
      assert.deepEqual(yield* overlay.resolve(instanceId, configured), configured);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("does not affect another provider instance", () =>
    Effect.gen(function* () {
      const overlay = yield* ProviderEnvironmentOverlay;
      const unrelatedId = ProviderInstanceId.make("codex-unrelated");
      yield* overlay.load({ instanceId, value: "selected-session" });
      assert.deepEqual(yield* overlay.resolve(unrelatedId, []), []);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("rejects empty and oversized values without exposing them", () =>
    Effect.gen(function* () {
      const overlay = yield* ProviderEnvironmentOverlay;
      const invalidInputs = [
        { instanceId, value: "" },
        { instanceId, value: "x".repeat(65_537) },
      ];

      for (const input of invalidInputs) {
        const error = yield* Effect.flip(overlay.load(input));
        assert.equal(error._tag, "ProviderValidationError");
        if (input.value.length > 0) {
          assert.equal(error.message.includes(input.value), false);
        }
      }
    }).pipe(Effect.provide(layer)),
  );
});
