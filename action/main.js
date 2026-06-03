// hestia-cache action, main entry point.
//
// Runs as a JS action because the GHA cache tokens (ACTIONS_RUNTIME_TOKEN,
// ACTIONS_RESULTS_URL) are only injected into the environment of JS actions,
// never into `run:` steps -- and because only JS actions get a native
// `post:` hook for the drain step.
//
// No npm dependencies on purpose: node builtins plus workflow commands
// replace @actions/core, so the action needs no bundling step.

'use strict';

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { spawn, spawnSync } = require('child_process');

// ---------------------------------------------------------------------------
// Tiny replacements for @actions/core
// ---------------------------------------------------------------------------

/** Export an environment variable to this process and all later job steps. */
function exportVariable(name, value) {
  process.env[name] = value;
  fs.appendFileSync(process.env.GITHUB_ENV, `${name}=${value}\n`);
}

/** Read an action input (the runner exposes them as INPUT_* variables). */
function getInput(name) {
  return (process.env[`INPUT_${name.toUpperCase()}`] || '').trim();
}

/**
 * Save a value for this invocation's post step (the runner exposes it there
 * as STATE_<name>). Unlike exported environment variables, state is not
 * shared between invocations: a job that runs this action twice gets two
 * post steps, each draining its own daemon.
 */
function saveState(name, value) {
  fs.appendFileSync(process.env.GITHUB_STATE, `${name}=${value}\n`);
}

function fail(message) {
  console.error(`::error::${message}`);
  process.exit(1);
}

/** Run a command, streaming output; throws on non-zero exit. */
function run(command, args) {
  const result = spawnSync(command, args, { stdio: 'inherit' });
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(' ')} exited with ${result.status}`);
  }
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// ---------------------------------------------------------------------------
// Setup steps
// ---------------------------------------------------------------------------

/** Capture the cache API tokens and re-export them for later shell steps. */
function captureTokens() {
  const token = process.env.ACTIONS_RUNTIME_TOKEN || '';
  const resultsUrl = process.env.ACTIONS_RESULTS_URL || '';
  if (!token || !resultsUrl) {
    fail(
      'ACTIONS_RUNTIME_TOKEN / ACTIONS_RESULTS_URL are not present in the action ' +
        'environment; hestia cannot talk to the GitHub Actions cache API'
    );
  }
  // The runtime token is a credential: mask it in logs before exporting.
  console.log(`::add-mask::${token}`);
  exportVariable('ACTIONS_RUNTIME_TOKEN', token);
  exportVariable('ACTIONS_RESULTS_URL', resultsUrl);
  console.log('hestia-cache: cache tokens captured and exported');
}

/**
 * Verify a downloaded release binary against GitHub's attestation API.
 *
 * The lookup is scoped to `repo` and keyed by content digest, so a match
 * proves the binary was built by that repository's release workflow. The
 * Sigstore signature is not checked (that needs a full Sigstore client);
 * the trust anchor is the GitHub API over TLS, the same anchor the release
 * download relies on.
 */
async function verifyAttestation(repo, assetName, digest, token) {
  const url = `https://api.github.com/repos/${repo}/attestations/sha256:${digest}`;
  const headers = {
    Accept: 'application/vnd.github+json',
    'X-GitHub-Api-Version': '2022-11-28',
  };
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }
  const response = await fetch(url, { headers });
  if (!response.ok) {
    fail(`attestation lookup failed: HTTP ${response.status} for ${url}`);
  }
  const attestations = (await response.json()).attestations || [];
  if (attestations.length === 0) {
    fail(
      `no build attestation found for ${assetName} (sha256:${digest}) in ${repo}; ` +
        'refusing to run an unverified binary'
    );
  }

  // The API does not always inline the bundle (sometimes there is only a
  // compressed bundle_url), so logging the building workflow is best effort.
  let builtBy = '';
  for (const attestation of attestations) {
    try {
      const statement = JSON.parse(
        Buffer.from(attestation.bundle.dsseEnvelope.payload, 'base64').toString('utf8')
      );
      const workflow = statement.predicate.buildDefinition.externalParameters.workflow;
      builtBy = `, built by ${workflow.repository}/${workflow.path}@${workflow.ref}`;
      break;
    } catch {
      // No inline bundle; the digest lookup above already verified.
    }
  }
  console.log(`hestia-cache: attestation verified for ${assetName} (sha256:${digest})${builtBy}`);
}

/** Install the hestia binary into installDir; returns its path. */
async function installBinary(installDir) {
  const target = path.join(installDir, 'hestia');
  const binary = getInput('binary');
  const version = getInput('version');

  if (binary) {
    console.log(`hestia-cache: installing from local binary ${binary}`);
    fs.copyFileSync(binary, target);
  } else {
    const arch = { x64: 'x86_64', arm64: 'aarch64' }[process.arch] || process.arch;
    // GITHUB_ACTION_REPOSITORY points at the repo this action was loaded
    // from, so forks automatically download their own releases.
    const repo = process.env.GITHUB_ACTION_REPOSITORY || 'Mic92/hestia';
    const assetName = `hestia-${arch}-linux`;
    const url = `https://github.com/${repo}/releases/download/${version}/${assetName}`;
    console.log(`hestia-cache: downloading ${url}`);
    const response = await fetch(url, { redirect: 'follow' });
    if (!response.ok) {
      fail(`download failed: HTTP ${response.status} for ${url}`);
    }
    const data = Buffer.from(await response.arrayBuffer());
    const digest = crypto.createHash('sha256').update(data).digest('hex');
    await verifyAttestation(repo, assetName, digest, getInput('github-token'));
    fs.writeFileSync(target, data);
  }
  fs.chmodSync(target, 0o755);
  return target;
}

/** Write the post-build-hook shim (Nix needs a program, not a subcommand). */
function writeHookShim(installDir, hestiaBin, socket) {
  const shim = path.join(installDir, 'post-build-hook');
  fs.writeFileSync(
    shim,
    '#!/bin/sh\n' +
      '# Forwards $OUT_PATHS of every local build to the hestia daemon.\n' +
      '# Always exits 0: a failing post-build-hook would fail the build itself.\n' +
      `exec "${hestiaBin}" hook --socket "${socket}"\n`
  );
  fs.chmodSync(shim, 0o755);
  return shim;
}

/**
 * Wire hestia into nix.conf:
 *
 * - ?trusted=true   -> Nix accepts unsigned narinfos from this substituter
 *                      (hestia serves locally-built, unsigned paths).
 * - ?priority=30    -> ahead of cache.nixos.org (40): locally-built paths
 *                      come from hestia, everything else from upstream.
 * - fallback = true -> if a cached path disappears mid-job (LRU eviction),
 *                      Nix rebuilds instead of failing.
 */
function configureNix(listen, hookShim) {
  const snippet =
    '\n# --- added by the hestia-cache action ---\n' +
    `extra-substituters = http://${listen}?trusted=true&priority=30\n` +
    `post-build-hook = ${hookShim}\n` +
    'fallback = true\n';

  const systemConf = '/etc/nix/nix.conf';
  if (fs.existsSync(systemConf)) {
    // Multi-user install (GitHub-hosted runners): the nix-daemon reads
    // /etc/nix/nix.conf and must be restarted to pick up the hook.
    appendPrivileged(systemConf, snippet);
    restartNixDaemon();
  } else {
    // Single-user install: per-user configuration is enough.
    const userConfDir = path.join(process.env.HOME || '/root', '.config', 'nix');
    fs.mkdirSync(userConfDir, { recursive: true });
    fs.appendFileSync(path.join(userConfDir, 'nix.conf'), snippet);
  }
}

/** Append to a root-owned file: direct write if permitted, sudo tee otherwise. */
function appendPrivileged(file, content) {
  try {
    fs.appendFileSync(file, content);
    return;
  } catch {
    // Not writable by this user; fall through to sudo.
  }
  const tee = spawnSync('sudo', ['tee', '-a', file], {
    input: content,
    stdio: ['pipe', 'ignore', 'inherit'],
  });
  if (tee.status !== 0) {
    fail(`cannot write ${file} (tried direct write and sudo tee)`);
  }
}

function restartNixDaemon() {
  const isActive = spawnSync('systemctl', ['is-active', '--quiet', 'nix-daemon']);
  if (isActive.status === 0) {
    console.log('hestia-cache: restarting nix-daemon to pick up nix.conf changes');
    run('sudo', ['systemctl', 'restart', 'nix-daemon']);
  }
}

/**
 * Extra `hestia serve` flags from optional inputs. Only emitted when set,
 * so older release binaries (which lack these flags) keep working with the
 * default inputs.
 */
function serveFlags() {
  const flags = [];
  if (getInput('upstream-cache-filter') === 'true') {
    flags.push('--upstream-cache-filter');
  }
  for (const name of getInput('upstream-cache-key-names').split(/\s+/).filter(Boolean)) {
    flags.push('--upstream-cache-key-name', name);
  }
  if (getInput('no-closure') === 'true') {
    flags.push('--no-closure');
  }
  return flags;
}

/** Start `hestia serve` detached so it outlives this action step. */
function startDaemon(hestiaBin, listen, socket, logFile) {
  const log = fs.openSync(logFile, 'a');
  const args = ['serve', '--listen', listen, '--socket', socket, ...serveFlags()];
  const daemon = spawn(hestiaBin, args, {
    detached: true,
    stdio: ['ignore', log, log],
    env: process.env, // carries ACTIONS_RUNTIME_TOKEN / ACTIONS_RESULTS_URL
  });
  daemon.unref();
  console.log(`hestia-cache: daemon started (pid ${daemon.pid}, log ${logFile})`);
}

/** Poll /nix-cache-info until the substituter answers (max ~30s). */
async function waitForReadiness(listen, logFile) {
  for (let attempt = 0; attempt < 60; attempt++) {
    try {
      const response = await fetch(`http://${listen}/nix-cache-info`);
      if (response.ok) {
        console.log(`hestia-cache: substituter ready at http://${listen}`);
        return;
      }
    } catch {
      // Not up yet.
    }
    await sleep(500);
  }
  console.error('--- hestia serve log ---');
  console.error(fs.readFileSync(logFile, 'utf8'));
  fail('hestia did not become ready within 30s');
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  captureTokens();

  const binary = getInput('binary');
  const version = getInput('version');
  if (!binary && !version) {
    console.log(
      'hestia-cache: neither `binary` nor `version` input set; ' +
        'token capture only (no daemon started, nothing will be cached)'
    );
    return;
  }

  const listen = getInput('listen') || '127.0.0.1:37515';
  const socket = getInput('socket') || '/tmp/hestia/hook.sock';
  // Unique per invocation: a job can run this action more than once, and a
  // shared directory would overwrite the first daemon's binary and log.
  const tempDir = process.env.RUNNER_TEMP || '/tmp';
  fs.mkdirSync(tempDir, { recursive: true });
  const installDir = fs.mkdtempSync(path.join(tempDir, 'hestia-cache-'));
  const logFile = path.join(installDir, 'serve.log');

  const hestiaBin = await installBinary(installDir);
  const hookShim = writeHookShim(installDir, hestiaBin, socket);
  configureNix(listen, hookShim);
  startDaemon(hestiaBin, listen, socket, logFile);
  await waitForReadiness(listen, logFile);

  // Environment variables for the user's later shell steps. When the action
  // runs more than once in a job, these point at the latest daemon.
  exportVariable('HESTIA_BIN', hestiaBin);
  exportVariable('HESTIA_SOCKET', socket);
  exportVariable('HESTIA_LISTEN', listen);
  exportVariable('HESTIA_DRAIN_TIMEOUT', getInput('drain-timeout') || '300');
  exportVariable('HESTIA_SERVE_LOG', logFile);
  fs.appendFileSync(process.env.GITHUB_PATH, `${installDir}\n`);

  // State for this invocation's own post step.
  saveState('bin', hestiaBin);
  saveState('socket', socket);
  saveState('serveLog', logFile);
  saveState('drainTimeout', getInput('drain-timeout') || '300');
}

main().catch((error) => {
  fail(error.stack || String(error));
});
