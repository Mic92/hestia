// hestia-cache action, post-job step.
//
// Runs after the job finished (whatever its outcome): tells the daemon to
// upload all locally-built paths and commit the manifest, then prints what
// happened. A failed drain marks this post step as failed so it is visible,
// but it cannot change the job's outcome (post steps never can).

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
    return 0;
  }

  const socket = getState('socket');
  const timeout = getState('drainTimeout') || '300';

  console.log('hestia-cache: draining (uploading built paths, committing the manifest)');
  const drain = spawnSync(binary, ['drain', '--socket', socket, '--timeout', timeout], {
    stdio: 'inherit',
  });

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
    console.error('::error::hestia drain failed; the paths built by this job were not cached');
  }
  return drain.status === null ? 1 : drain.status;
}

process.exit(main());
