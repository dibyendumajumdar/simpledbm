# Lock Management #

  * J.N.Gray, R.A.Lorie, G.R.Putzolu, I.L.Traiger. Granularity of Locks and Degrees of consistency in a Shared Data Base. Readings in Database Systems, Third Edition, 1998. Morgan Kaufmann Publishers.
  * Jim Gray and Andreas Reuter. Chapter 7: Isolation Concepts. Transaction Processing: Concepts and Techniques. Morgan Kaufmann Publishers, 1993.
  * Jim Gray. Notes on Database Operating Systems, IBM Technical Report RJ2188, 1978. Also published in Operating systems: An Advanced Course. Springer-Verlag Lecture Notes in Computer Science. Vol. 60. pp. 393-481. 1978.
  * Jim Gray and Andreas Reuter. Chapter 8: Lock Implementation. Transaction Processing: Concepts and Techniques. Morgan Kaufmann Publishers, 1993.
  * K. P. Eswaran, J. N. Gray. R. A. Lorie. and I. L. Traiger. The Notions of Consistency and Predicate Locks in a Database System. Communications of the ACM. 19(11):624-633, November 1976.

# Write Ahead Log #

  * Jim Gray and Andreas Reuter. Chapter 9: Log Manager. Transaction Processing: Concepts and Techniques. Morgan Kaufmann Publishers, 1993
  * R.J.Peterson and J.P.Strickland. Log write-ahead protocols and IMS/VS logging. Proceedings of the 2nd ACM SIGACT-SIGMOD symposium on Principles of database systems, Pages: 216 - 243, 1983
  * Raymond A. Lorie, Physical integrity in a large segmented database, ACM Transactions on Database Systems (TODS), v.2 n.1, p.91-104, March 1977.

# Buffer Management #

  * Wolfgang Effelsberg and Theo Haerder. Principles of Database Buffer Management. ACM TODS 9(4): 560-595.
  * G.M.Sacco and M.Shokolnick. Buffer Management in Relational Database Systems. ACM TODS 11(4): 473-498.
  * Elizabeth J. O'Neil , Patrick E. O'Neil , Gerhard Weikum. The LRU-K page replacement algorithm for database disk buffering. ACM SIGMOD 22(2) 1993.
  * Jim Gray and Andreas Reuter. Chapter 13: File and Buffer Management. Transaction Processing: Concepts and Techniques. Morgan Kaufmann Publishers, 1993

# Storage Management #

  * C.Mohan and D.Haderle. Algorithms for Flexible Space Management in Transaction Systems Supporting Fine-Granularity Locking. In Proceedings of the International Conference on Extending Database Technology, March 1994
  * Jim Gray and Andreas Reuter. Chapter 13: File and Buffer Management. Transaction Processing: Concepts and Techniques. Morgan Kaufmann Publishers, 1993
  * Jim Gray and Andreas Reuter. Chapter 14: The Tuple-Oriented File System. Transaction Processing: Concepts and Techniques. Morgan Kaufmann Publishers, 1993
  * Mark L.McAuliffe, Michael J. Carey and Marvin H. Solomon. Towards Effective and Efficient Free Space Management. ACM SIGMOD Record. Proceedings of the 1996 ACM SIGMOD international conference on Management of Data, June 1996.
  * Dan Hotka. Oracle8i GIS (Geeky Internal Stuff): Physical Data Storage Internals. OracleProfessional, September, 1999.

# Transaction Management and Recovery #

  * R.A.Crus. Data recovery in IBM Database 2. IBM Systems Journal, Vol 23, No 2, 1984.
  * Theo Haerder and Andreas Reuter. Principles of Transaction-Oriented Database Recovery. ACM Computing Surveys, 15(4), pp. 287-318, December, 1983. Also in Readings in Database Systems, Third Edition, 1998. Morgan Kaufmann Publishers.
  * Jim Gray and Andreas Reuter. Chapter 10: Transaction Manager Concepts. Transaction Processing: Concepts and Techniques. Morgan Kaufmann Publishers, 1993
  * C. Mohan, D. Haderle, B. Lindsay, H. Pirahesh and P. Schwarz. ARIES: A Transaction Recovery Method Supporting Fine-Granularity Locking and Partial Rollbacks Using Write-Ahead Logging. ACM Transactions on Database Systems, 17(1):94-162, March 1992. Also, Readings in Database Systems, Third Edition, 1998. Morgan Kaufmann Publishers.
  * Jim Gray and Andreas Reuter. Chapter 11: Transaction Manager Structure. Transaction Processing: Concepts and Techniques. Morgan Kaufmann Publishers, 1993

# Multiversion Concurrency #

  * Michael Stonebraker. The Design of the Postgres Storage System. Proceedings 13th International Conference on Very Large Data Bases (brighton, Sept, 1987). Also, Readings in Database Systems, Third Edition, 1998. Morgan Kaufmann Publishers.
  * Authors unknown. The Postgres Access Methods. Postgres V4.2 distribution.
  * Bruce Momijian. PostgreSQL Internals through Pictures. Dec 2001. http://www.postgresql.org.
  * C.Mohan, Hamid Pirahesh, Raymond Lorie. Efficient and Flexible methods for transient versioning of records to avoid locking by read-only transactions. ACM SIGMOD, V21 N2, P124-133, June 1 1992.
  * Roger Bamford and Kenneth Jacobs, Oracle.US Patent Number 5870758: Method and Apparatus for providing Isolation Levels in a Database System. Feb, 1999.
  * Dan Hotka. Oracle8i GIS (Geeky Internal Stuff): Rollback Segment Internals. OracleProfessional, May, 2001.
  * Bill Todd. InterBase: What sets is apart. http://www.ibphoenix.com.
  * Paul Beach. InterBase and the Oldest Interesting Transaction. http://www.ibphoenix.com.
  * Jim Starkey. Indexes, Multigenerations and Everything. Posting dated 28 June, 2000, on IB-architect mailing list.

# BTree Index Management #

  * Rudolf Bayer, Edward M. McCreight. Organization and Maintenance of Large Ordered Indices. Acta Inf. 1: 173-189 (1972).
  * Douglas Comer. The Ubiquitous B-Tree. ACM Computing Surveys. 11(2): 121-137.
  * C.Mohan and Frank Levine. ARIES/IM: an efficient and high concurrency index management method with write-ahead logging. ACM SIGMOD Record. V21 N2, P371-380, June 1 1992.
  * Jim Gray and Andreas Reuter. Chapter 15: Access Paths. Transaction Processing: Concepts and Techniques. Morgan Kaufmann Publishers, 1993
  * P. L. Lehman and S. B. Yao. Efficient locking for concurrent operations on B-Trees. ACM Transactions on Database Systems, 6(4):650-670, December 1981. Also in Readings in Database Systems, Third Edition, 1998. Morgan Kaufmann Publishers.
  * David Lomet and Betty Salzburg. Access method concurrency with recovery. ACM SIGMOD, V21 N2, p351-360, June 1 1992.
  * Dan Hotka. Oracle8i GIS (Geeky Internal Stuff): Index Internals. OracleProfessional, November, 2000.
  * Ibrahim Jaluta, Seppo Sippu and Eljas Soisalon-Soininen. [Concurrency control and recovery for balanced B-link trees](http://www.springerlink.com/openurl.asp?genre=article&issn=1066-8888&volume=14&issue=2&spage=257). The VLDB Journal, Volume 14, [Issue 2](https://code.google.com/p/simpledbm/issues/detail?id=2) (April 2005), Pages: 257 - 277, ISSN:1066-8888.
  * Mohan, C. An Efficient Method for Performing Record Deletions and Updates Using Index Scans, Proc. 28th International Conference on Very Large Databases, Hong Kong, August 2002.