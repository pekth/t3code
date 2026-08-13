/**
 * ProviderInstanceRegistryHydration - derive a `ProviderInstanceConfigMap`
 * from `ServerSettings` and keep `ProviderInstanceRegistry` in sync with it.
 *
 * The server still reads two shapes:
 *
 *   1. `settings.providerInstances` - the new driver-agnostic map the
 *      registry expects. Keyed by `ProviderInstanceId`, values are
 *      `ProviderInstanceConfig` envelopes.
 *   2. `settings.providers.<kind>` - the legacy single-instance-per-driver
 *      fields (`providers.codex`, `providers.claudeAgent`, …). These are
 *      the source of truth for every deployment that hasn't been migrated
 *      yet to an explicit `providerInstances` entry.
 *
 * This module bridges (2) into (1) and wires the resulting map into a
 * mutable registry. For every built-in driver whose id is not already
 * present in `providerInstances` (keyed on
 * `defaultInstanceIdForDriver(driverKind)` - literally the driver kind as a
 * routing slug), we synthesize an envelope from the legacy field. The
 * registry decodes both flavours through the same `configSchema` and ends
 * up with one uniform `ProviderInstance` per entry.
 *
 * Explicit `providerInstances` entries always win - users can already
 * override the legacy `providers.<kind>` blob by authoring a
 * `providerInstances.codex` entry with a matching driver, and we don't
 * want the synthesized envelope to silently stomp their config.
 *
 * Hot-reload
 * ----------
 * On layer build we:
 *   1. Read the current `ServerSettings` once and use it to seed the
 *      registry's initial state via `ProviderInstanceRegistryMutableLayer`.
 *   2. Fork a daemon fiber (lifetime tied to the layer's scope) that
 *      subscribes to `ServerSettingsService.streamChanges` and calls
 *      `ProviderInstanceRegistryMutator.reconcile` on every emission.
 *
 * Failures inside the watcher are logged and swallowed so a single bad
 * settings emission cannot kill the registry. Unknown drivers and invalid
 * configs already round-trip through the registry's own "unavailable"
 * shadow bucket.
 *
 * @module provider/Layers/ProviderInstanceRegistryHydration
 */
import {
  defaultInstanceIdForDriver,
  ProviderInstanceId,
  type ProviderInstanceConfig,
  type ProviderInstanceConfigMap,
  ServerSettings,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";

import { ServerConfig } from "../../config.ts";
import { startEphemeralProviderEnvIpcServer } from "../../localIpc/ephemeralProviderEnv.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { BUILT_IN_DRIVERS, type BuiltInDriversEnv } from "../builtInDrivers.ts";
import { ProviderEnvironmentOverlay } from "../Services/ProviderEnvironmentOverlay.ts";
import { ProviderInstanceRegistry } from "../Services/ProviderInstanceRegistry.ts";
import { ProviderInstanceRegistryMutator } from "../Services/ProviderInstanceRegistryMutator.ts";
import { layer as ProviderEnvironmentOverlayLive } from "./ProviderEnvironmentOverlay.ts";
import { ProviderInstanceRegistryMutableLayer } from "./ProviderInstanceRegistryLive.ts";

/**
 * Synthesize a `ProviderInstanceConfigMap` from a `ServerSettings` snapshot.
 *
 * Strategy:
 *   1. Copy all explicit `settings.providerInstances` entries verbatim.
 *   2. For each built-in driver whose `defaultInstanceIdForDriver(id)` key
 *      is *not* already in the explicit map, synthesize an entry from the
 *      matching legacy `settings.providers.<kind>` blob.
 *
 * The returned map is the input the registry consumes; pure & exported
 * separately so the hydration logic can be exercised by unit tests
 * without layering.
 */
export const deriveProviderInstanceConfigMap = (
  settings: ServerSettings,
): ProviderInstanceConfigMap => {
  const merged: Record<string, ProviderInstanceConfig> = { ...settings.providerInstances };

  for (const driver of BUILT_IN_DRIVERS) {
    const instanceId = defaultInstanceIdForDriver(driver.driverKind);
    if (instanceId in merged) {
      // Explicit `providerInstances` entry for this slot - user-authored
      // config always wins over the legacy mirror.
      continue;
    }

    // Only built-in drivers have a legacy mirror; the registry's
    // `providers` struct is keyed on the same literal slug as
    // `driverKind`. Access is dynamic (the driver kind is a branded string),
    // but it's constrained to `keyof settings.providers` by the union of
    // built-in driver kinds.
    const legacyKey = driver.driverKind as keyof ServerSettings["providers"];
    const legacyConfig = settings.providers[legacyKey];
    if (legacyConfig === undefined) {
      continue;
    }

    merged[instanceId] = {
      driver: driver.driverKind,
      config: legacyConfig,
    };
  }

  return merged as ProviderInstanceConfigMap;
};

const deriveProviderInstanceConfigMapWithOverlay = Effect.fn(
  "ProviderInstanceRegistryHydration.deriveProviderInstanceConfigMapWithOverlay",
)(function* (settings: ServerSettings) {
  const overlay = yield* ProviderEnvironmentOverlay;
  const configured = deriveProviderInstanceConfigMap(settings);
  const resolved: Record<string, ProviderInstanceConfig> = {};

  for (const [instanceId, entry] of Object.entries(configured)) {
    const environment = yield* overlay.resolve(
      ProviderInstanceId.make(instanceId),
      entry.environment ?? [],
    );
    resolved[instanceId] =
      environment === entry.environment ||
      (entry.environment === undefined && environment.length === 0)
        ? entry
        : { ...entry, environment };
  }

  return resolved as ProviderInstanceConfigMap;
});

const reconcileProviderInstanceEnvironment = Effect.fn(
  "ProviderInstanceRegistryHydration.reconcileProviderInstanceEnvironment",
)(function* (instanceId: ProviderInstanceId) {
  const serverSettings = yield* ServerSettingsService;
  const mutator = yield* ProviderInstanceRegistryMutator;
  const overlay = yield* ProviderEnvironmentOverlay;
  const settings = yield* serverSettings.getSettings;
  const configured = deriveProviderInstanceConfigMap(settings);
  const entry = configured[instanceId];
  if (entry === undefined) return;
  const environment = yield* overlay.resolve(instanceId, entry.environment ?? []);
  const configMap = {
    ...configured,
    [instanceId]: { ...entry, environment },
  } as ProviderInstanceConfigMap;
  yield* mutator.reconcile(configMap);
});

/**
 * Layer that consumes `ProviderInstanceRegistryMutator` and forks a
 * settings-watcher fiber. The fiber's lifetime is tied to the enclosing
 * layer scope (process lifetime in production), so it is interrupted on
 * shutdown without leaking.
 *
 * Errors inside the watcher are logged and swallowed - the registry's own
 * "unavailable" bucket already absorbs unknown drivers and invalid
 * configs, so the only way the watcher could fail is a settings stream
 * tear-down, which logs and exits cleanly.
 */
const SettingsWatcherLive = Layer.effectDiscard(
  Effect.gen(function* () {
    const mutator = yield* ProviderInstanceRegistryMutator;
    const serverSettings = yield* ServerSettingsService;
    yield* serverSettings.streamChanges.pipe(
      Stream.runForEach((next) =>
        deriveProviderInstanceConfigMapWithOverlay(next).pipe(
          Effect.flatMap((configMap) => mutator.reconcile(configMap)),
          Effect.catchCause((cause) =>
            Effect.logError("ProviderInstanceRegistry reconcile failed", cause),
          ),
        ),
      ),
      Effect.forkScoped,
    );
  }),
);

const EphemeralProviderEnvironmentIpcLive = Layer.effectDiscard(
  Effect.gen(function* () {
    const config = yield* ServerConfig;
    const registry = yield* ProviderInstanceRegistry;
    const overlay = yield* ProviderEnvironmentOverlay;
    const handlerContext = yield* Effect.context<
      ServerSettingsService | ProviderInstanceRegistryMutator | ProviderEnvironmentOverlay
    >();

    yield* Effect.acquireRelease(
      Effect.tryPromise({
        try: () =>
          startEphemeralProviderEnvIpcServer({
            stateDir: config.stateDir,
            handler: {
              hasProviderInstance: (instanceId) =>
                Effect.runPromise(
                  registry
                    .getInstance(instanceId)
                    .pipe(Effect.map((instance) => instance !== undefined)),
                ),
              load: (input) =>
                Effect.runPromise(
                  overlay
                    .load(input)
                    .pipe(
                      Effect.andThen(reconcileProviderInstanceEnvironment(input.instanceId)),
                      Effect.provideContext(handlerContext),
                    ),
                ),
              clear: (instanceId) =>
                Effect.runPromise(
                  overlay
                    .clear(instanceId)
                    .pipe(
                      Effect.andThen(reconcileProviderInstanceEnvironment(instanceId)),
                      Effect.provideContext(handlerContext),
                    ),
                ),
            },
          }),
        catch: (cause) => cause,
      }).pipe(Effect.orDie),
      (server) =>
        Effect.promise(() => server.close()).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("Failed to close ephemeral provider environment IPC", { cause }),
          ),
        ),
    );
  }),
);

/**
 * Hydrate `ProviderInstanceRegistry` from `ServerSettings` and keep it in
 * sync with subsequent `streamChanges` emissions.
 *
 * The Layer's two halves:
 *   - `ProviderInstanceRegistryMutableLayer` produces the registry +
 *     mutator from the initial config map. Its scope owns every
 *     per-instance child scope created during reconcile.
 *   - `SettingsWatcherLive` consumes the mutator and runs a daemon fiber
 *     in the same scope.
 *
 * Composing via `Layer.provideMerge` makes the watcher's deps available
 * from the mutable layer while still surfacing the registry as an output.
 * The mutator tag is technically also exposed; only this module imports
 * it, so the visibility leak is harmless in practice.
 */
export const ProviderInstanceRegistryHydrationLive: Layer.Layer<
  ProviderInstanceRegistry,
  never,
  BuiltInDriversEnv | ServerSettingsService | ServerConfig
> = Layer.unwrap(
  Effect.gen(function* () {
    const serverSettings = yield* ServerSettingsService;
    const initialSettings: ServerSettings | undefined = yield* serverSettings.getSettings.pipe(
      Effect.orElseSucceed(() => undefined),
    );
    const initialConfigMap =
      initialSettings === undefined
        ? ({} as ProviderInstanceConfigMap)
        : yield* deriveProviderInstanceConfigMapWithOverlay(initialSettings);

    const mutableLayer = ProviderInstanceRegistryMutableLayer({
      drivers: BUILT_IN_DRIVERS,
      configMap: initialConfigMap,
    });

    return Layer.merge(SettingsWatcherLive, EphemeralProviderEnvironmentIpcLive).pipe(
      Layer.provideMerge(mutableLayer),
    );
  }),
).pipe(Layer.provide(ProviderEnvironmentOverlayLive)) as Layer.Layer<
  ProviderInstanceRegistry,
  never,
  BuiltInDriversEnv | ServerSettingsService | ServerConfig
>;
