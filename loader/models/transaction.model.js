'use strict';

class TransactionModel {
  constructor(raw) {
    const {txid, block, vins, vouts} = raw;
    this.txid = txid; //string
    this.block = block; //number
    this.vins = vins; //'txid:index'[]
    this.vouts = vouts; //'txid:index'[]
  }
}

class TransactionDependsRelationModel {
  constructor(raw) {
    const {parent, child, index} = raw;
    this.parent = parent; //TransactionModel
    this.child = child; //TransactionModel
    this.index = index; //number
    this.txid = parent.txid; //string
  }
}

module.exports = {
  TransactionModel,
  TransactionDependsRelationModel,
};
