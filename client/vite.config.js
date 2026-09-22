import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// In Docker the server is reachable by its compose service name.
const API_URL = process.env.API_URL ?? 'http://localhost:3001';

export default defineConfig({
  plugins: [react()],
  server: {
    // Listen on all interfaces so the port also works from inside a container.
    host: true,
    port: 5173,
    // One origin for the browser, so no CORS setup is needed.
    proxy: {
      '/api': API_URL,
      '/ws': { target: API_URL, ws: true },
    },
  },
});
