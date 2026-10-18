// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Render the real Fern site locally. No hosted preview or publish command runs.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import net from 'node:net';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';

const root = fileURLToPath(new URL('../../', import.meta.url));
const cli = fileURLToPath(new URL('node_modules/fern-api/cli.cjs', import.meta.url));
const pages = JSON.parse(readFileSync(new URL('pages.json', import.meta.url)));
const output = new URL('render-output/', import.meta.url);
mkdirSync(output, { recursive: true });

/** Select an unused loopback port for this preview's owned child process. */
async function freePort() {
  const server = net.createServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const { port } = server.address();
  await new Promise((resolve) => server.close(resolve));
  return port;
}

/** Exclude script payloads: serialized page data alone is not rendered content. */
function visibleText(html) {
  return html.replace(/<(script|style)\b[^>]*>[\s\S]*?<\/\1>/gi, '')
    .replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ');
}

const port = await freePort();
let backendPort = await freePort();
while (backendPort === port) backendPort = await freePort();
// The renderer rejects FERN_TOKEN even when its value is empty. It belongs
// only to hosted publication, never the local preview's child environment.
const renderEnv = { ...process.env, CI: 'true' };
delete renderEnv.FERN_TOKEN;
const child = spawn(process.execPath, [cli, 'docs', 'dev', '--port', String(port),
  '--backend-port', String(backendPort)], {
  cwd: root, detached: true, stdio: ['ignore', 'pipe', 'pipe'],
  env: renderEnv,
});
let log = '';
for (const stream of [child.stdout, child.stderr]) {
  stream.on('data', (chunk) => { log = (log + chunk).slice(-50000); });
}
let spawnError;
child.on('error', (error) => { spawnError = error; });
let cleanupPromise;

/** Stop our detached preview group once, including on terminal/CI signals. */
function cleanup() {
  cleanupPromise ??= (async () => {
    if (!child.pid) return;
    for (const signal of ['SIGTERM', 'SIGKILL']) {
      try { process.kill(-child.pid, signal); } catch (error) {
        if (error.code !== 'ESRCH') throw error;
      }
      if (signal === 'SIGTERM') await delay(500);
    }
  })();
  return cleanupPromise;
}

for (const [signal, code] of [['SIGINT', 130], ['SIGTERM', 143]]) {
  process.once(signal, async () => {
    try { await cleanup(); } catch (error) { console.error(error); }
    process.exit(code);
  });
}

const deadline = Date.now() + 180000;

/** Fetch a rendered route with bounded retries while the local bundle starts. */
async function pageAt(path) {
  let lastError;
  while (Date.now() < deadline) {
    if (spawnError) throw spawnError;
    if (child.exitCode !== null) throw new Error(`Fern exited ${child.exitCode}\n${log}`);
    try {
      const response = await fetch(`http://127.0.0.1:${port}${path}`, {
        signal: AbortSignal.timeout(10000), redirect: 'follow',
      });
      assert.equal(response.status, 200, `${path}: HTTP ${response.status}`);
      const html = await response.text();
      writeFileSync(new URL(path.slice(1).replaceAll('/', '--') + '.html', output), html);
      return html;
    } catch (error) { lastError = error; }
    await delay(1000);
  }
  throw new Error(`Local Fern render timed out: ${lastError}\n${log}`);
}

try {
  // Read the site's URL prefix (for example, /nvidia-recovery) from the
  // Overview navigation link and use it when requesting each page.
  const landing = await pageAt('/');
  const links = [...landing.matchAll(/href="([^"]+)"/g)].map((match) => match[1]);
  const overviewRoute = links.find((link) => link.startsWith('/') && link.endsWith('/overview'));
  assert.ok(overviewRoute, 'Overview link missing from rendered navigation');
  const basePath = overviewRoute.slice(0, -'/overview'.length);
  for (const { slug, marker } of pages) {
    const route = `${basePath}/${slug}`;
    assert.ok(links.includes(route), `${slug}: page link missing from rendered navigation`);
    const html = await pageAt(route);
    assert.ok(visibleText(html).includes(marker), `${slug}: rendered body is missing ${marker}`);
    assert.ok(!/Application error:|Internal Server Error|Page not found/i.test(visibleText(html)),
      `${slug}: error page rendered`);
    console.log(`Rendered ${route}`);
  }
  // Discover operation links from the rendered navigation, then require the
  // operation's own request/response content, beyond its sidebar label.
  for (const [prefix, operation, marker] of [
    ['gateway-reference', 'getjobs', 'pageToken'],
    ['gateway-reference', 'createauditevents', 'CreateAuditEventsRequest'],
    ['attribution-reference', 'attribution-v-1-attribution-post', 'workload_id'],
  ]) {
    const route = links.find((link) => link.startsWith(`${basePath}/${prefix}/`) && link.endsWith(`/${operation}`));
    assert.ok(route, `${prefix}: operation link missing from rendered navigation`);
    const html = await pageAt(route);
    const text = visibleText(html);
    assert.ok(text.includes(marker), `${route}: operation schema is not rendered`);
    assert.ok(text.includes('Response'), `${route}: response documentation is not rendered`);
    console.log(`Rendered ${route}`);
  }
  assert.ok(!/Failed to read docs configuration|Rendering blank page|\[error\]/.test(log), log);
  console.log('Local Fern render passed; hosted publication and live APIs were not exercised.');
} catch (error) {
  console.error(log);
  throw error;
} finally {
  // Artifact failures must never bypass termination of the owned processes.
  await cleanup();
  writeFileSync(new URL('fern.log', output), log);
}
