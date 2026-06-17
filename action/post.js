// hestia-cache action, post-job step.
//
// Runs after the job finished (whatever its outcome): tells the daemon to
// upload all locally-built paths and commit the manifest, then prints what
// happened. The cache is best-effort, so this step always exits 0: the build
// already succeeded, and any paths left uncached are simply rebuilt next time.
// A failed or timed-out drain is therefore surfaced as a GitHub warning and
// never fails the job — matching `hestia hook`, which also always exits 0.
// (A non-zero post-step exit *does* mark the job failed, so exiting 0 here is
// load-bearing, not incidental.)

'use strict';

const fs = require('fs');
const { spawnSync } = require('child_process');

/** Read a value the main step saved with saveState(). */
function getState(name) {
  return (process.env[`STATE_${name}`] || '').trim();
}

function main() {
  const binary = getState('bin');
  if (!binary) {
    console.log('hestia-cache: no daemon was started in this job; nothing to drain');
    return;
  }

  const socket = getState('socket');
  const timeout = getState('drainTimeout') || '300';

  console.log('hestia-cache: draining (uploading built paths, committing the manifest)');
  const drain = spawnSync(binary, ['drain', '--socket', socket, '--timeout', timeout], {
    stdio: 'inherit',
  });
  if (drain.error) {
    // spawnSync does not throw on launch failures (e.g. ENOENT when the
    // temp dir was cleaned mid-job); without this the only output is the
    // generic drain-failed warning below.
    console.warn(`::warning::failed to run ${binary}: ${drain.error}`);
  }

  // The daemon log carries what the drain summary does not: per-stage drain
  // timings, substituter hits, and error details. Collapsed on success so it
  // does not bury the summary; printed plainly on failure.
  const log = getState('serveLog');
  if (log && fs.existsSync(log)) {
    if (drain.status === 0) {
      console.log('::group::hestia daemon log');
    } else {
      console.log('--- hestia daemon log ---');
    }
    console.log(fs.readFileSync(log, 'utf8'));
    if (drain.status === 0) {
      console.log('::endgroup::');
    }
  }

  if (drain.status !== 0) {
    console.warn('::warning::hestia drain failed; the paths built by this job were not cached (the build is unaffected; they will be rebuilt next time)');
  }

  // The daemon is spawned detached and never exits on its own; on
  // persistent self-hosted runners it would outlive the job, holding its
  // port and a revoked runtime token. SIGTERM triggers a graceful final
  // drain before exit.
  const pid = parseInt(getState('daemonPid'), 10);
  if (pid > 0) {
    try {
      process.kill(pid, 'SIGTERM');
      console.log(`hestia-cache: daemon (pid ${pid}) terminated`);
    } catch {
      // Already gone.
    }
  }
}

main();

// Always 0: a failed drain is a warning, not a job failure (see the header).
// Not process.exit(): that drops pending async stdout writes, truncating the
// daemon log dump exactly when it is needed.
process.exitCode = 0;
