// Variant C. demo.json and series.json are served from the one UI export (publicDir), and the
// visibility rule is imported from ../shared (the same tested port variant B uses).
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  base: "./",
  publicDir: "../demo/live-week-2/ui",
  server: { fs: { allow: [".."] } },
  build: { outDir: "dist", emptyOutDir: true },
});
