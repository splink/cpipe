# Cassandra JSON exporter

Export Cassandra tables to json.

Compile with:
~~~bash
sbt stage
~~~

Execute
~~~bash
./target/universal/stage/bin/extr --hosts localhost,... -k keyspace -t "table" --progress > ouput
~~~
