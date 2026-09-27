import { defineConfig } from 'tsdown'

/** Build the provider entry and declaration file for npm publication. */
export default defineConfig({
  entry: ['src/index.ts'],
  outDir: 'lib',
  format: ['esm'],
  platform: 'node',
  target: 'es2024',
  dts: true,
  clean: true,
  fixedExtension: false,
  deps: { neverBundle: [/^@deepseek-ai\//, 'pg'] },
})
