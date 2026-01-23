// Node wrapper to launch Vite with crypto shims in non-browser contexts.
const crypto = require("crypto");
const path = require("path");
const { pathToFileURL } = require("url");

if (!globalThis.crypto && crypto.webcrypto) {
  globalThis.crypto = crypto.webcrypto;
}

if (!crypto.getRandomValues && crypto.webcrypto?.getRandomValues) {
  crypto.getRandomValues = crypto.webcrypto.getRandomValues.bind(crypto.webcrypto);
}

const cliPath = pathToFileURL(
  path.resolve(__dirname, "../node_modules/vite/bin/vite.js")
).href;

import(cliPath);
