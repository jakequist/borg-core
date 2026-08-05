/**
 * The runtime half of the `paths` mapping in `tsconfig.json`.
 *
 * `test/generated/borg.generated.ts` is `borg generate`'s output, checked in unedited, so it imports
 * `borg-sdk/client` — the specifier a user's project would use. tsc resolves that through `paths`;
 * vitest needs to be told separately, because it resolves modules itself.
 */

import { defineConfig } from "vitest/config";
import { fileURLToPath } from "node:url";

export default defineConfig({
  resolve: {
    alias: {
      "borg-sdk/client": fileURLToPath(new URL("./src/client.ts", import.meta.url)),
    },
  },
});
