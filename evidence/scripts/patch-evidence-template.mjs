import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

const templateRoots = [
  resolve("node_modules", "@evidence-dev", "evidence", "template"),
  resolve(".evidence", "template"),
];

const appCss = `:root {
  font-family: Inter, ui-sans-serif, system-ui, sans-serif;
  color: #172033;
  background: #ffffff;
}

body {
  margin: 0;
}

a {
  color: #2563eb;
}

.markdown {
  line-height: 1.55;
}
`;

for (const templateRoot of templateRoots) {
  if (!existsSync(templateRoot)) {
    continue;
  }

  const layoutPath = resolve(templateRoot, "src", "pages", "+layout.svelte");
  const fontsImport = "\timport '@evidence-dev/tailwind/fonts.css';\n";
  const currentLayout = readFileSync(layoutPath, "utf8");

  if (currentLayout.includes(fontsImport)) {
    writeFileSync(layoutPath, currentLayout.replace(fontsImport, ""), "utf8");
  }

  writeFileSync(resolve(templateRoot, "src", "app.css"), appCss, "utf8");

  const viteConfigPath = resolve(templateRoot, "vite.config.js");
  const viteConfig = readFileSync(viteConfigPath, "utf8")
    .replace(/\n\s*import tailwindcss from '@tailwindcss\/vite';\n/, "\n")
    .replace("plugins: [tailwindcss(), sveltekit(),", "plugins: [sveltekit(),");

  writeFileSync(viteConfigPath, viteConfig, "utf8");
}
