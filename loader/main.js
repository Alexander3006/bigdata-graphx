'use strict';

const block_start = 8143000;
const block_finish = 8143100
let current = block_start;

(async () => {
  const application = await require('./application');

  while(current <= block_finish) {
    const result = await application.loaderService.loadBlock({height: current});
    console.log(`${current}:${result}`);
    if(result) current++
  }

  const shutdown = async () => {
    await application.stop();
    process.exit(0);
  };

  process.on('uncaughtException', (err) => console.error(err));
  process.on('unhandledRejection', (err) => console.error(err));
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
})();
