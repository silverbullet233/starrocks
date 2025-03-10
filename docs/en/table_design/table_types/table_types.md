---
displayed_sidebar: docs
---

# Overview of table types

You must specify a table type and define one or more columns as a sort key at table creation. This way, when data is initially loaded into the table that you created, StarRocks can sort, process, and store the data based on the sort key. This topic describes the table types that StarRocks provides to meet your varying business requirements.

## Basic concepts

### Table types

StarRocks provides four table types: Duplicate Key table, Aggregate table, Unique Key table, and Primary Key table. These four table types are well suited to a wide range of data analytics scenarios such as log analysis, data aggregation and analysis, and real-time data analysis.

### Sort keys

When data is loaded into a table created by using a certain table type, data is sorted and stored according to one or more columns defined as the sort key when the table is created. The sort key is usually one or more columns that are frequently used as filter conditions in queries, thereby accelerating queries.

Since v3.0, Primary Key tables support defining sort keys using `ORDER BY`. Since v3.3, Duplicate Key tables, Aggregate tables, and Unique Key tables support defining sort keys using `ORDER BY`.

For more descriptions of sort keys, see [Sort keys and prefix indexes](../indexes/Prefix_index_sort_key.md).

## Precautions

- After a table is created, you cannot modify its table types. For example, you cannot change a Duplicate Key table to a Primary Key table. You must create a new Primary Key table.

- Sort key columns must be defined prior to the other columns in the statement for table creation.

- The order of sort key columns in the statement for table creation specifies the order of the conditions based on which the rows in the table are sorted.

- The length of the prefix index for a table is limited to 36 bytes. If the total length of the sort key columns exceeds 36 bytes, StarRocks stores only the first few sort key columns within the length limit for the prefix index.

- If the records to be loaded into a table have the same primary key, StarRocks processes and stores the records based on the table type:
  - Duplicate Key table

    StarRocks loads each of the records as a separate row into the table. After the data load is complete, the table contains rows that have the same primary key, and the rows map the source records in a one-to-one relationship. You can recall all historical data that you loaded.

  - Aggregate table

    StarRocks aggregates the records into one record and loads the aggregated record as a row into the table. After the loading is complete, the table does not contain rows that have the same primary key. You can recall the aggregation results of all historical data that you loaded. However, you cannot recall all historical data.

  - Unique Key table and Primary Key table

    StarRocks replaces each previously loaded record with the newly loaded record and retains only the most recently loaded record as a row in the table. After the loading is complete, the table does not contain previous rows that have the same primary key. Only the latest record that contains the same unique key/primary key is retained. The Unique Key table and the Primary Key table can be considered a special Aggregate table in which the REPLACE aggregate function is specified for metric columns to return the most recent record among a group of records that have the same primary key.
