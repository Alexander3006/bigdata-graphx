'use strict';

const {GraphQLClient, gql} = require('graphql-request');
const {
  AccountModel,
  AccountSendRelationModel,
  AccountReceiveRelationModel,
} = require('../../models/account.model');
const {
  TransactionModel,
  TransactionDependsRelationModel,
} = require('../../models/transaction.model');

const NATIVE_TOKEN_FINGERPRINT = 'NATIVE_TOKEN';

class CardanoClientError extends Error {}

class CardanoClient {
  constructor({node}) {
    this.node = node;
    const client = new GraphQLClient(node, {
      // timeout: 120000,
    });
    this.provider = client;
  }

  async getBlock({height}) {
    try {
      const rawBlock = await this.getRawBlock({height});
      const block = this.parseBlock(rawBlock);
      return block;
    } catch (err) {
      console.error(err);
      throw new CardanoClientError('Error getting parsed block');
    }
  }

  async getRawBlock({height}) {
    const {provider} = this;
    try {
      const query = gql`
        query ($height: Int!) {
          transactions(where: {block: {number: {_eq: $height}}}) {
            block {
              number
            }
            hash
            inputs {
              address
              sourceTxHash
              sourceTxIndex
              txHash
              tokens {
                asset {
                  fingerprint
                }
                quantity
              }
              value
              sourceTransaction {
                block {
                  number
                }
                hash
                inputs {
                  sourceTxIndex
                  sourceTxHash
                }
                outputs {
                  index
                  txHash
                }
              }
            }
            outputs {
              address
              index
              value
              txHash
              tokens {
                asset {
                  fingerprint
                }
                quantity
              }
            }
          }
        }
      `;
      const {transactions} = await provider.request(query, {height}).catch((err) => {
        const transactions = err?.response?.data?.transactions;
        if (!transactions) throw err;
        const corrected = transactions.filter((transaction) => {
          const {outputs} = transaction;
          const broken = outputs.some((output) => !output?.asset);
          return !broken;
        });
        return {transactions: corrected};
      });
      if (!transactions[0]?.block?.number) return null;
      return {transactions};
    } catch (err) {
      console.error(err);
      throw new CardanoClientError('Error getting raw block from node');
    }
  }

  parseBlock(rawBlock) {
    const {transactions = []} = rawBlock ?? {};
    const parsed = transactions.map((transaction) => {
      const {
        inputs,
        outputs,
        hash,
        block: {number: block},
      } = transaction;
      const vins = inputs.map(
        ({sourceTxHash, sourceTxIndex}) => `${sourceTxHash}:${sourceTxIndex}`,
      );
      const vouts = outputs.map(({txHash, index}) => `${txHash}:${index}`);
      //transaction
      const transactionModel = new TransactionModel({
        txid: hash,
        vins,
        vouts,
        block,
      });
      //transaction depends relation
      const transactionDependsRelationModels = inputs.map((input) => {
        const {sourceTransaction, sourceTxIndex} = input;
        const {
          hash,
          block: {number: block},
        } = sourceTransaction;
        const vins = sourceTransaction.inputs.map(
          ({sourceTxHash, sourceTxIndex}) => `${sourceTxHash}:${sourceTxIndex}`,
        );
        const vouts = sourceTransaction.outputs.map(({txHash, index}) => `${txHash}:${index}`);
        const parentTransactionModel = new TransactionModel({
          txid: hash,
          block,
          vins,
          vouts,
        });
        const transactionDependsRelationModel = new TransactionDependsRelationModel({
          child: transactionModel,
          parent: parentTransactionModel,
          index: sourceTxIndex,
        });
        return transactionDependsRelationModel;
      });
      //account send relations
      const accountSendRelationModels = inputs.map((input) => {
        const {address, sourceTxHash, sourceTxIndex, value, tokens} = input;
        const account = new AccountModel({address});
        const payload = [
          ...tokens.map(({quantity, asset: {fingerprint: token}}) => `${token}:${quantity}`),
          `${NATIVE_TOKEN_FINGERPRINT}:${value}`,
        ];
        const accountSendRelationModel = new AccountSendRelationModel({
          sender: account,
          transaction: transactionModel,
          txid: sourceTxHash,
          index: sourceTxIndex,
          payload,
        });
        return accountSendRelationModel;
      });
      //account receive relations
      const accountReceiveRelationModels = outputs.map((output) => {
        const {address, txHash, index, value, tokens} = output;
        const account = new AccountModel({address});
        const payload = [
          ...tokens.map(({quantity, asset: {fingerprint: token}}) => `${token}:${quantity}`),
          `${NATIVE_TOKEN_FINGERPRINT}:${value}`,
        ];
        const accountSendRelationModel = new AccountReceiveRelationModel({
          recipient: account,
          transaction: transactionModel,
          txid: txHash,
          index,
          payload,
        });
        return accountSendRelationModel;
      });
      //summary
      return {
        accounts: [
          ...accountReceiveRelationModels.map(({recipient}) => recipient),
          ...accountSendRelationModels.map(({sender}) => sender),
        ],
        transactions: [transactionModel],
        relations: {
          send: accountSendRelationModels ?? [],
          receive: accountReceiveRelationModels ?? [],
          depends: transactionDependsRelationModels ?? [],
        },
      };
    });
    //aggregate
    const result = parsed.reduce(
      (result, parsed) => {
        const aggregated = {
          accounts: [...result.accounts, ...parsed.accounts],
          transactions: [...result.transactions, ...parsed.transactions],
          relations: {
            send: [...result.relations.send, ...parsed.relations.send],
            receive: [...result.relations.receive, ...parsed.relations.receive],
            depends: [...result.relations.depends, ...parsed.relations.depends],
          },
        };
        return aggregated;
      },
      {
        accounts: [],
        transactions: [],
        relations: {
          send: [],
          receive: [],
          depends: [],
        },
      },
    );
    //
    return result;
  }
}

module.exports = {
  CardanoClient,
  CardanoClientError,
};
