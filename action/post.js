// hestia-cache action, post-job step.
//
// Runs after the job finished (whatever its outcome): tells the daemon to
// upload all locally-built paths and commit the manifest, then prints what
// happened. A failed drain marks this post step as failed so it is visible,
// but it cannot change the job's outcome (post steps never can).

'use strict';

const fs = require('fs');
const { spawnSync } = require('child_process');

function main() {
  const binary = process.env.HESTIA_BIN || '';
  if (!binary) {
    console.log('hestia-cache: no daemon was started in this job; nothing to drain');
    return 0;
  }

  const socket = process.env.HESTIA_SOCKET || '/tmp/hestia/hook.sock';
  const timeout = process.env.HESTIA_DRAIN_TIMEOUT || '300';

  console.log('hestia-cache: draining (uploading built paths, committing the manifest)');
  const drain = spawnSync(binary, ['drain', '--socket', socket, '--timeout', timeout], {
    stdio: 'inherit',
  });

  if (drain.status !== 0) {
    // Show the daemon log: the drain summary alone rarely explains failures.
    const log = process.env.HESTIA_SERVE_LOG || '';
    if (log && fs.existsSync(log)) {
      console.log('--- hestia serve log ---');
      console.log(fs.readFileSync(log, 'utf8'));
    }
    console.error('::error::hestia drain failed; the paths built by this job were not cached');
  }
  return drain.status === null ? 1 : drain.status;
}

process.exit(main());
