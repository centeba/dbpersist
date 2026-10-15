import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Built assets are served by the dashboard at /app/, so paths are relative to that base.
// In dev, /api is proxied to the dashboard so the browser talks to one origin.
export default defineConfig({
  plugins: [react()],
  base: "/app/",
  server: {
    port: 5174,
    proxy: {
      "/api": { target: "http://localhost:9200", changeOrigin: true },
    },
  },
});
