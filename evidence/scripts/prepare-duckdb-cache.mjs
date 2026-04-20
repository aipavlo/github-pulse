import { mkdirSync } from "node:fs";
import { join } from "node:path";

const extensionCacheDir = join(
  process.env.HOME,
  ".duckdb",
  "extensions",
  "extensions.duckdb.org",
  "v1.4.3",
  "wasm_eh",
);

mkdirSync(extensionCacheDir, { recursive: true });
