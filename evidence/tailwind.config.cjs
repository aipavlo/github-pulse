const evidenceTailwind = require("@evidence-dev/tailwind/config").config;

/** @type {import("tailwindcss").Config} */
module.exports = {
  content: [
    "./.evidence/template/src/**/*.{html,js,svelte,ts,md}",
    "./pages/**/*.{md,svelte}",
    "./partials/**/*.{md,svelte}",
    "./node_modules/@evidence-dev/core-components/dist/**/*.{html,js,svelte,ts,md}",
  ],
  presets: [evidenceTailwind],
  theme: {
    extend: {},
  },
  plugins: [],
};
