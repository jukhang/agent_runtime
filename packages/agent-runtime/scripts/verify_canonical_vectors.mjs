#!/usr/bin/env node
// Independent JavaScript implementation of RFC 8785 (JCS) used to verify the Python
// canonical JSON rule in packages/agent-runtime. Two modes:
//
//   node scripts/verify_canonical_vectors.mjs [fixture.json]
//       Re-derive every golden vector's text and SHA-256 digest and compare.
//       Exits non-zero on the first mismatch; prints a JSON summary on success.
//
//   node scripts/verify_canonical_vectors.mjs --numbers | --strings
//       Read a JSON array of numbers or strings from stdin and print a JSON array of
//       their ECMAScript texts. tests/test_canonical.py uses these as oracles for
//       randomized doubles and Unicode strings; JSON.stringify is the specification.
//
// The serializer is written by hand on purpose: JSON.stringify on an object would emit
// property order, and ECMAScript orders integer-like keys before other keys instead of
// sorting every key by UTF-16 code unit as JCS requires.

import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

function canonical(value) {
  if (value === null) return "null";
  switch (typeof value) {
    case "boolean":
      return value ? "true" : "false";
    case "number":
      if (!Number.isFinite(value)) throw new Error("non-finite number");
      return JSON.stringify(value);
    case "string":
      return JSON.stringify(value);
    case "object": {
      if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
      // Array.prototype.sort() compares UTF-16 code units, which is the JCS order.
      const keys = Object.keys(value).sort();
      return `{${keys.map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`).join(",")}}`;
    }
    default:
      throw new Error(`unsupported value: ${typeof value}`);
  }
}

function digest(text) {
  return createHash("sha256").update(Buffer.from(text, "utf8")).digest("hex");
}

function readStdin() {
  return readFileSync(0, "utf8");
}

function verifyVectors(path) {
  const document = JSON.parse(readFileSync(path, "utf8"));
  const failures = [];
  for (const vector of document.vectors) {
    const text = canonical(vector.value);
    const sha = digest(text);
    if (text !== vector.canonical) {
      failures.push({ name: vector.name, field: "canonical", expected: vector.canonical, actual: text });
    }
    if (sha !== vector.digest) {
      failures.push({ name: vector.name, field: "digest", expected: vector.digest, actual: sha });
    }
  }
  if (failures.length > 0) {
    console.error(JSON.stringify({ status: "failed", spec: document.spec, failures }, null, 2));
    process.exit(1);
  }
  console.log(
    JSON.stringify({
      status: "passed",
      spec: document.spec,
      vectors: document.vectors.length,
      digests: document.vectors.length,
    }),
  );
}

function oracleMode(kind) {
  const values = JSON.parse(readStdin());
  if (!Array.isArray(values)) throw new Error(`${kind} expects a JSON array`);
  console.log(JSON.stringify(values.map((value) => JSON.stringify(value))));
}

const here = dirname(fileURLToPath(import.meta.url));
const args = process.argv.slice(2);
if (args[0] === "--numbers" || args[0] === "--strings") {
  oracleMode(args[0]);
} else {
  verifyVectors(resolve(process.cwd(), args[0] ?? resolve(here, "../src/agent_runtime/contracts/fixtures/canonical-vectors.json")));
}
