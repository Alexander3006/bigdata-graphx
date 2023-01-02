'use strict'

class LoadrerServiceError extends Error {}

class LoadrerService {
  constructor({cardano, db}) {
    this.cardano = cardano;
    this.db = db;
  }

  async loadBlock({height}) {
    const {cardano} = this;
    try {
      const block = await cardano.getBlock({height});
      if(!block) return false;
      await this.saveBlock(block);
      return true;
    } catch(err) {
      console.error(err);
      throw new LoadrerServiceError(`Block loading error: ${height}`)
    }
  }

  async saveBlock(block) {
    const {db: {graph}} = this;
    const transaction = await graph.transaction();
    const {
      AccountRepository,
      AccountSendRelationRepository,
      AccountReceiveRelationRepository,
      TransactionRepository,
      TransactionDependsRelationRepository,
    } = transaction.repositories;
    try {
      const {
        accounts,
        transactions,
        relations: {
          send,
          receive,
          depends,
        },
      } = block;
      
      // console.log(`Accounts`, accounts.length);
      // console.log(`Transactions`, transactions.length);
      // console.log(`Send`, send.length);
      // console.log(`Receive`, receive.length);
      // console.log(`Depends`, depends.length);
      
      await Promise.all(accounts.map(account => AccountRepository.insert(account)));
      await Promise.all(transactions.map(transaction => TransactionRepository.insert(transaction)));
      await Promise.all(send.map(relation => AccountSendRelationRepository.insert(relation)));
      await Promise.all(receive.map(relation => AccountReceiveRelationRepository.insert(relation)));
      await Promise.all(depends.map(relation => TransactionDependsRelationRepository.insert(relation)));

      await transaction.commit();
      return;
    } catch(err) {
      await transaction.rollback();
      console.error(err);
      throw new LoadrerServiceError('Block saving error');
    } finally {
      transaction.release();
    }
  }
}

module.exports = {
  LoadrerService,
  LoadrerServiceError,
}