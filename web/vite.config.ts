import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    host: "0.0.0.0",
    port: 5173,
    proxy: {
      "/v1": process.env.VITE_PROXY_TARGET ?? "http://localhost:3000",
      "/health": process.env.VITE_PROXY_TARGET ?? "http://localhost:3000",
    },
  },
});
