simpleETL
=========

A command line ETL utility, originally intended for simple ETL requirements, namely ones only required extraction and loading. 

Due to increasing complex requirements to detect changes in the source system it has evolved to detect changes in the source systems in a myriad of ways, including:
- New records based on a "key > old_key"
- Updated record based on "column > old_column"
- PK appears in a column of new records in a related tabled

The primary application for this has been between Oracle and Vertica.