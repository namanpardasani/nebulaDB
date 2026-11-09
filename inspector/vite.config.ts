import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'

// The admin server (internal/admin) allows CORS from loopback origins, so the
// dev server talks to it directly; the address is chosen at runtime with
// ?admin=http://127.0.0.1:7380 (see src/api/client.ts).
export default defineConfig({
  plugins: [react()],
  server: { host: '127.0.0.1', port: 5173 },
  test: { environment: 'jsdom', include: ['src/**/*.test.ts', 'src/**/*.test.tsx'] },
})
