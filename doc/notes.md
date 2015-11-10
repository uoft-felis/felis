# Motivation

Database replication in wide area.

* Guaranteed Network Bandwidth in wide area is expensive.
* Database is getting faster -- more data need to be sent every second.

# Approach

Send the "input parameters" to the transactions, and re-execute the transactions on the replica to generate the same data. In another word: rather than sending the data, can we send the execution?

# Assumptions

* Isolation level have to be Serializable.
* Database API
  * Index and Tables are BTrees
  * API of BTrees: get/put/scan
  * Other API: start_tx/end_tx
  * Transaction: is a C/C++ written function running in the database accessing these APIs.

# Algorithms

## Step 1: Collecting Data on the Primary Node

Divide all committed transaction into epochs.

Sort the transactions in Serializability Order.

For each transaction, we need to send:

* Input parameters for a transaction function
* Where the transaction write to, including: [^step1-note]
  * Which table/index
  * What key in that table/index
  * No need to send the data

[^step1-note]: We're hoping for: size of transaction parameters < size of data the transaction generates.

## Step 2: Setting up the Versions



## Step 3: Re-Executing Transactions
