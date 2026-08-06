import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// `VITE_API` is set by dev.sh so that the ui and the api agree on a port without either hard-coding
// the other's. Nothing else is configured: no proxy, no aliases, no build step for the api.
export default defineConfig({
  plugins: [react()],
});
