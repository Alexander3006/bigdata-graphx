'use strinct';

const TransactionRepository = (connection) => ({
  insert: async (transactionModel) => {
    const query = "MERGE (transaction:Transaction {txid: $txid, block: $block, vins: $vins, vouts: $vouts})";
    await connection.run(query, {
      ...transactionModel,
    });
    return;
  },
});

const TransactionDependsRelationRepository = (connection) => ({
  insert: async (transactionDependsRelationModel) => {
    const query = `
      MATCH (child:Transaction {txid: $c_txid}), (parent:Transaction {txid: $p_txid})
      MERGE (child) -[:Depends {txid: $d_txid, index: $d_index}]-> (parent)
    `;
    await connection.run(query, {
      c_txid: transactionDependsRelationModel.child.txid,
      p_txid: transactionDependsRelationModel.parent.txid,
      d_txid: transactionDependsRelationModel.txid,
      d_index: transactionDependsRelationModel.index,
    });
    return;
  },
});

module.exports = {
  TransactionRepository,
  TransactionDependsRelationRepository,
};
