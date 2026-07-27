import { createRequire } from 'node:module';

import { ExampleEvaluator } from 'texample';

const require = createRequire(import.meta.url);
const packageDefinition = require('../package.json');

let exitCode = 0;
try {
  await new ExampleEvaluator('./README.md', packageDefinition, process.cwd()).evaluate();
} catch (err) {
  console.error(err.stack ?? err);
  exitCode = 1;
}

// examples may leave fake servers bound, exit explicitly
process.exit(exitCode);
