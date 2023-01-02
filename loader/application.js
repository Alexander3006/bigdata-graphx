'use strict';

const application = (async () => {
  const config = {
    graphdburl: 'neo4j://localhost',
    cardno_node: 'https://graphql-api.mainnet.dandelion.link',
  };

  const GraphDb = require('./infrastructure/graphdb/connection');
  const graphDbConnection = await GraphDb.connect({url: config.graphdburl});

  const Repositories = require('./infrastructure/graphdb/repositories');
  const {UnitOfWork} = require('./infrastructure/graphdb/unit-of-work');
  const unitOfWork = new UnitOfWork({models: Repositories, connection: graphDbConnection});
  const db = {
    graph: unitOfWork
  };

  const {CardanoClient} = require('./infrastructure/cardano');
  const cardano = new CardanoClient({
    node: config.cardno_node,
  });

  const {LoadrerService} = require('./services/loader.service');
  const loaderService = new LoadrerService({cardano, db});

  return {
    db,
    cardano,

    loaderService,

    stop: async () => {
      await Promise.allSettled([graphDbConnection.close()]);
    },
  };
})().catch((err) => {
  console.error(err);
  process.exit(1);
});

module.exports = application;
