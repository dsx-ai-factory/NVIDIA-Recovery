// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { cpSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const root = fileURLToPath(new URL('../../', import.meta.url));
const checker = fileURLToPath(new URL('check.mjs', import.meta.url));
const cli = fileURLToPath(new URL('node_modules/fern-api/cli.cjs', import.meta.url));

/** Isolate deliberate defects so CI never mutates the source checkout. */
function fixture() {
  const temp = mkdtempSync(path.join(os.tmpdir(), 'recovery-fern-negative-'));
  cpSync(path.join(root, 'fern'), path.join(temp, 'fern'), { recursive: true });
  cpSync(path.join(root, 'api-gateway/gen/openapiv2'), path.join(temp, 'api-gateway/gen/openapiv2'), { recursive: true });
  return temp;
}

/** Run the pinned validator with a bounded duration and return its diagnostics. */
function check(cwd, args) {
  const result = spawnSync(process.execPath, args ? [cli, ...args] : [checker], {
    cwd, encoding: 'utf8', timeout: 60000, env: { ...process.env, FERN_TOKEN: '', CI: 'true' },
  });
  assert.ifError(result.error);
  return { status: result.status, output: result.stdout + result.stderr };
}

test('CLI lock and project version agree', () => {
  const config = JSON.parse(readFileSync(path.join(root, 'fern/fern.config.json')));
  const pkg = JSON.parse(readFileSync(new URL('package.json', import.meta.url)));
  const lock = JSON.parse(readFileSync(new URL('package-lock.json', import.meta.url)));
  assert.match(config.version, /^\d+\.\d+\.\d+$/);
  assert.equal(config.version, pkg.devDependencies['fern-api']);
  assert.equal(config.version, lock.packages['node_modules/fern-api'].version);
});

test('real Fern check accepts valid documentation without hosted credentials', () => {
  const temp = fixture();
  try {
    const result = check(temp);
    assert.equal(result.status, 0, result.output);
  } finally { rmSync(temp, { recursive: true, force: true }); }
});

test('real Fern check rejects a broken internal page link', () => {
  const temp = fixture();
  try {
    const page = path.join(temp, 'fern/generated/overview.mdx');
    writeFileSync(page, readFileSync(page, 'utf8') + '\n[Broken](/does-not-exist-for-validation)\n');
    const result = check(temp);
    assert.notEqual(result.status, 0, result.output);
    assert.match(result.output, /does-not-exist-for-validation/);
  } finally { rmSync(temp, { recursive: true, force: true }); }
});

test('real Fern check rejects a broken OpenAPI reference', () => {
  const temp = fixture();
  try {
    const spec = path.join(temp, 'fern/apis/attribution-adapter/openapi.json');
    const data = JSON.parse(readFileSync(spec));
    data.paths['/v1/attribution'].post.requestBody.content['application/json'].schema = {
      $ref: '#/components/schemas/MissingForValidation',
    };
    writeFileSync(spec, JSON.stringify(data));
    const result = check(temp);
    assert.notEqual(result.status, 0, result.output);
    assert.match(result.output, /MissingForValidation/);
  } finally { rmSync(temp, { recursive: true, force: true }); }
});

test('real MDX validation rejects malformed markup', () => {
  const temp = fixture();
  try {
    writeFileSync(path.join(temp, 'fern/generated/overview.mdx'), '# Broken MDX\n\n<Callout>\n');
    const result = check(temp, ['docs', 'md', 'check']);
    assert.notEqual(result.status, 0, result.output);
  } finally { rmSync(temp, { recursive: true, force: true }); }
});
