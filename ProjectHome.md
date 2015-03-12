In database replication scenario, it is often useful to identify the possible differences between two databases. This is the purpose of pgddiff.

It compares one database to another one, the reference, at different level:
  * 0. table and sequence name
  * 1. number of records and sequence current value
  * 2. column description (name and type)
  * 3. primary key
  * 4. primary key value
  * 5. record data