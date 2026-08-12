import type { ProviderInstanceEnvironment, ProviderInstanceId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type { ProviderValidationError } from "../Errors.ts";

export interface LoadProviderBitwardenSessionInput {
  readonly instanceId: ProviderInstanceId;
  readonly value: string;
}

export interface ProviderEnvironmentOverlayShape {
  /** Loads or replaces the in-memory Bitwarden session for one provider instance. */
  readonly load: (
    input: LoadProviderBitwardenSessionInput,
  ) => Effect.Effect<void, ProviderValidationError>;

  readonly clear: (instanceId: ProviderInstanceId) => Effect.Effect<void>;

  /** Resolves the in-memory BW_SESSION over the configured instance environment. */
  readonly resolve: (
    instanceId: ProviderInstanceId,
    configured: ProviderInstanceEnvironment,
  ) => Effect.Effect<ProviderInstanceEnvironment>;
}

export class ProviderEnvironmentOverlay extends Context.Service<
  ProviderEnvironmentOverlay,
  ProviderEnvironmentOverlayShape
>()("t3/provider/Services/ProviderEnvironmentOverlay") {}
