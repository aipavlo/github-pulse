import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

const templateRoots = [
  resolve("node_modules", "@evidence-dev", "evidence", "template"),
  resolve(".evidence", "template"),
];

const appCss = `@tailwind base;
@tailwind components;
@tailwind utilities;

:root {
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

  if (!currentLayout.includes(fontsImport)) {
    writeFileSync(
      layoutPath,
      currentLayout.replace("import '../app.css';\n", `import '../app.css';\n${fontsImport}`),
      "utf8",
    );
  }

  writeFileSync(resolve(templateRoot, "src", "app.css"), appCss, "utf8");
}
