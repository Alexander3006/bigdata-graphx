CREATE CONSTRAINT transaction_txid_uniq
FOR (n:Transaction)
REQUIRE n.txid IS UNIQUE;

CREATE CONSTRAINT account_address_uniq
FOR (n:Account)
REQUIRE n.address IS UNIQUE;


