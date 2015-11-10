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
  * API of BTrees: ``Get()``/``Put()``/``Scan()``
  * Other API: ``StartTx()``/``EndTx()``
  * Transaction: is a C/C++ written function running in the database accessing these APIs.

# Algorithms

## Step 1: Collecting Data on the Primary Node

Divide all committed transaction into epochs.

Sort the transactions in *Serializability Order*: the order the transactions were to run in serial to produce the same outcome.

For each transaction, we need to send:

* Input parameters for a transaction function
* Where the transaction write to, including: [^step1-note]
  * Which table/index
  * What key in that table/index
  * No need to send the data

[^step1-note]: We're hoping for: size of transaction parameters < size of data the transaction generates.

## Step 2: Setting up the Versions

Step 2 is on the backup node.

Since we sort the transactions in Serializability order, we name these transactions with a seralizable-id. Transaction ``tid`` means: if transactions were to execute in serial, transaction ``tid`` is the ``tid``-th transaction that gets executed.

In a transaction, for each keys that produced on the primary node, insert the key with a dummy value into the index. If the key exist, then create a new version of the data (which is always a dummy value). Version number = serializable-id. 

## Step 3: Re-Executing Transactions

On the backup node, we now can re-execute transactions in parallel.

1. We re-execute the transaction functions, with input parameters we gathered in step 1.
2. When the transaction function need to read a key
   * Search the key in the index, and get all possible versions of data in this key.
   * Binary search the serializable-id of this transaction in the all possible versions
     * for example all possible version is [3 9 12], this transaction is 8, then it will find 3<8<9
   * Read the data with the maximum-smaller version number (which is the serializable-id of the writer)
     * In the above example, transaction 8 is going to read the version transaction 3 had written
   * When the value of that version is a dummy value, then wait.
3. When the transaction function need to write a key
   * Search the key in the index, find version number = serializable-id of the current transaction
   * Write to it
     * Replacing the dummy value with the value the transaction function need to write [^step3-note]

[^step3-note]: Notice that each version of the data is immutable after this write.

## Garbage Collection on the Backup Node

After step 3, a lot old versions of data are no longer in use. However, they are still in the index. We need to clean them up.

One possible strategy: when step 1 for the next epoch, we clean up the old versions lazily for each key.