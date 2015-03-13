At present SimpleDBM does not support XA transactions and cannot therefore participate in distributed transactions. To support XA, following changes are needed:

  * A way of associating Xids with SimpleDBM transactions. SimpleDBM transactions are currently identified using a monotonically increasing 64-bit integer.
  * XAResource API calls do not always have equivalents in SimpleDBM, particularly the semantics of start() and end(). It is relatively easy to implement forget() and recover().
  * At present the prepare() call is not exposed in the SimpleDBM API.
  * SimpleDBM does not re-acquire locks for prepared transactions after recovery because at present there is no need for it. In an XA environment, it must re-acquire all exclusive locks that were held at the time the transaction was prepared.
  * SimpleDBM has no concept of transaction timeout.

Splitting out the prepare() call must be tested properly as it is currently part of the commit processing and not a separate step.