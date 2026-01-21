import { webcrypto } from "node:crypto";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

if (!globalThis.crypto) {
  globalThis.crypto = webcrypto as any;
}

export default defineConfig({
  plugins: [react()],
  server: {
    host: true,
    port: 5173,
    proxy: {
      "/api": "http://localhost:8000",
    },
  },
});
