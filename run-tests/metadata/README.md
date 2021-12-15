We can find out the schema used to create the tables, by issue: show create table <table name>

To invoke the creation of all of the tables, use the following command:

clickhouse-client -h <host> --multiquery --multiline --receive_timeout 300 < ./testing_related_tables_creation.sql
