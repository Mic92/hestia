// Deprecated Mic92/hestia/action entry point. The action moved to the repo
// root so it can be listed on the GitHub Marketplace. Warn, then run the
// real implementation.

'use strict';

console.log(
  '::warning::Mic92/hestia/action is deprecated; use Mic92/hestia instead'
);

require('./main.js');
