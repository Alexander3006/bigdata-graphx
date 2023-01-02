'use strict';

class AccountModel {
  constructor(raw) {
    const {address} = raw;
    this.address = address;
  }
}

class AccountSendRelationModel {
  constructor(raw) {
    const {sender, transaction, index, payload} = raw;
    this.sender = sender; //AccountModel
    this.transaction = transaction; //TransactionModel
    this.txid = transaction.txid; //string
    this.index = index; //number
    this.payload = payload; //'token:amount'[]
  }
}

class AccountReceiveRelationModel {
  constructor(raw) {
    const {recipient, transaction, index, payload} = raw;
    this.recipient = recipient; //AccountModel
    this.transaction = transaction; //TransactionModel
    this.txid = transaction.txid; //string
    this.index = index; //number
    this.payload = payload; //'token:amount'[]
  }
}

module.exports = {
  AccountModel,
  AccountSendRelationModel,
  AccountReceiveRelationModel,
};
