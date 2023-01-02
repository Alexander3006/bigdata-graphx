'use strict';

const neo4j = require('neo4j-driver');

const connect = async ({url}) => {
  const driver = neo4j.driver(url, undefined, {
    maxConnectionPoolSize: 20,
  });
  const session = driver.session();
  return session;
};

module.exports = {
  connect,
};
