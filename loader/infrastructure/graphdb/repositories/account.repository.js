'use strinct';

const AccountRepository = (connection) => ({
  insert: async (accountModel) => {
    const query = "MERGE (account:Account {address: $address})"
    await connection.run(query, {
      ...accountModel,
    });
    return;
  },
});

const AccountSendRelationRepository = (connection) => ({
  insert: async (accountSendRelationModel) => {
    const query = `
      MATCH (account:Account {address: $a_sender}), (transaction:Transaction {txid: $t_txid})
      MERGE (account) -[relation:Send {txid: $r_txid, index: $r_index, payload: $r_payload }]-> (transaction)
    `
    await connection.run(query, {
      a_sender: accountSendRelationModel.sender.address,
      r_txid:  accountSendRelationModel.txid,
      r_index: accountSendRelationModel.index,
      r_payload: accountSendRelationModel.payload,
      t_txid: accountSendRelationModel.transaction.txid,
    });
    return;
  },
});

const AccountReceiveRelationRepository = (connection) => ({
  insert: async (accountReceiveRelationModel) => {
    const query = `
      MATCH (transaction:Transaction {txid: $t_txid}), (account:Account {address: $a_recipient})
      MERGE (transaction) -[relation:Receive {txid: $r_txid, index: $r_index, payload: $r_payload }]-> (account)
    `;
    await connection.run(query, {
      a_recipient: accountReceiveRelationModel.recipient.address,
      r_txid:  accountReceiveRelationModel.txid,
      r_index: accountReceiveRelationModel.index,
      r_payload: accountReceiveRelationModel.payload,
      t_txid: accountReceiveRelationModel.transaction.txid,
    });
    return;
  },
});

module.exports = {
  AccountRepository,
  AccountSendRelationRepository,
  AccountReceiveRelationRepository,
};
