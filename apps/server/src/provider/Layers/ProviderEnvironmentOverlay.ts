import {
  ProviderInstanceEnvironmentVariableName,
  type ProviderInstanceEnvironment,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";

import { ProviderValidationError } from "../Errors.ts";
import {
  ProviderEnvironmentOverlay,
  type ProviderEnvironmentOverlayShape,
} from "../Services/ProviderEnvironmentOverlay.ts";

export const PROVIDER_ENVIRONMENT_OVERLAY_VALUE_MAX_CHARS = 65_536;
const BITWARDEN_SESSION_NAME = ProviderInstanceEnvironmentVariableName.make("BW_SESSION");

const OverlayValue = Schema.String.check(
  Schema.isNonEmpty(),
  Schema.isMaxLength(PROVIDER_ENVIRONMENT_OVERLAY_VALUE_MAX_CHARS),
);
type OverlayState = ReadonlyMap<ProviderInstanceId, string>;

const invalidInput = (operation: string, issue: string) =>
  new ProviderValidationError({ operation, issue });

const decodeValue = (value: string) =>
  Schema.decodeUnknownEffect(OverlayValue)(value).pipe(
    Effect.mapError(() =>
      invalidInput(
        "ProviderEnvironmentOverlay.load",
        `value must contain between 1 and ${PROVIDER_ENVIRONMENT_OVERLAY_VALUE_MAX_CHARS} characters.`,
      ),
    ),
  );

export const make = Effect.gen(function* () {
  const state = yield* Ref.make<OverlayState>(new Map());

  const load: ProviderEnvironmentOverlayShape["load"] = Effect.fn(
    "ProviderEnvironmentOverlay.load",
  )(function* (input) {
    const value = yield* decodeValue(input.value);
    yield* Ref.update(state, (current) => {
      const next = new Map(current);
      next.set(input.instanceId, value);
      return next;
    });
  });

  const clear: ProviderEnvironmentOverlayShape["clear"] = (instanceId) =>
    Ref.update(state, (current) => {
      const next = new Map(current);
      next.delete(instanceId);
      return next;
    });

  const resolve: ProviderEnvironmentOverlayShape["resolve"] = Effect.fn(
    "ProviderEnvironmentOverlay.resolve",
  )(function* (instanceId, configured) {
    const value = yield* Ref.get(state).pipe(Effect.map((current) => current.get(instanceId)));
    if (value === undefined) return configured;

    const resolved = new Map(configured.map((variable) => [variable.name, variable]));
    resolved.set(BITWARDEN_SESSION_NAME, {
      name: BITWARDEN_SESSION_NAME,
      value,
      sensitive: true,
    });
    return Array.from(resolved.values()) satisfies ProviderInstanceEnvironment;
  });

  return ProviderEnvironmentOverlay.of({ load, clear, resolve });
});

export const layer = Layer.effect(ProviderEnvironmentOverlay, make);
