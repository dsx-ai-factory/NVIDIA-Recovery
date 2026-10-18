// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Fern warns (rather than fails) on some invalid OpenAPI references. Fail all
// diagnostics except the authoritative Gateway Swagger warning and the expected
// skipped hosted redirect check in credential-free local/PR validation.
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const cli = fileURLToPath(new URL('node_modules/fern-api/cli.cjs', import.meta.url));
const result = spawnSync(process.execPath, [cli, 'check', '--local', '--json'], {
  encoding: 'utf8', timeout: 60000, env: { ...process.env, FERN_TOKEN: '' },
});
if (result.error) throw result.error;
// Fern prints its human summary before the structured object in this version.
const start = result.stdout.indexOf('{');
if (start === -1) throw new Error(result.stdout + result.stderr);
const report = JSON.parse(result.stdout.slice(start));
const diagnostics = Object.values(report.results).flat();
for (const item of diagnostics) console.log(`${item.severity}: ${item.message}`);
const rejected = diagnostics.filter((item) => !(
  item.severity === 'warning' && (
    item.rule === 'OpenAPI v2.0 detected' || (
      item.rule === 'missing-redirects' &&
      item.message.startsWith('Missing redirects check skipped: not authenticated.')
    )
  )
));
if (result.status !== 0 || !report.success || rejected.length > 0) process.exitCode = 1;
