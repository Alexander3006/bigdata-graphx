'use strict'

'use strict';

class UnitOfWorkError extends Error {}

class UnitOfWork {
  #isTransaction = false;
  #models;
  constructor({models, connection, transaction = false}) {
    this.factories = models;
    this.connection = connection;
    const builtModels = this.#build(models, connection);
    this.#models = builtModels;
    this.#isTransaction = transaction;
  }

  get repositories() {
    const {connection} = this;
    if (!connection) throw new UnitOfWorkError('Connection released');
    return {...this.#models};
  }

  #build(models, connection) {
    const builtModels = Object.keys(models).reduce((builtModels, name) => {
      const modelFactory = models[name];
      const model = modelFactory(connection);
      return Object.assign(builtModels, {[name]: model});
    }, {});
    return builtModels;
  }

  async transaction() {
    const {connection, factories} = this;
    if (!connection) throw new UnitOfWorkError('Connection released');
    if (this.#isTransaction) throw new UnitOfWorkError('Transaction has already been created');
    const transaction = await connection.beginTransaction();
    return new UnitOfWork({
      models: factories,
      connection: transaction,
      transaction: true,
    });
  }

  async commit() {
    const {connection} = this;
    if (!connection) throw new UnitOfWorkError('Connection released');
    if (!this.#isTransaction) throw new UnitOfWorkError('Transaction not created');
    await connection.commit();
    return this;
  }

  async rollback() {
    const {connection} = this;
    if (!connection) throw new UnitOfWorkError('Connection released');
    if (!this.#isTransaction) throw new UnitOfWorkError('Transaction not created');
    await connection.rollback();
    return this;
  }

  release() {
    const {connection} = this;
    if (!connection) throw new UnitOfWorkError('Connection released');
    if (!this.#isTransaction) throw new UnitOfWorkError('Transaction not created');
    this.connection = null;
    this.factories = null;
    return null;
  }

  query(query, ...args) {
    const {connection} = this;
    if (!connection) throw new UnitOfWorkError('Connection released');
    return connection.run(query, args);
  }
}

module.exports = {
  UnitOfWork,
  UnitOfWorkError,
};
