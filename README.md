# Cassandra JSON import/export

Export Cassandra tables to json.
Import Cassandra tables from json.

Compile with:
~~~bash
sbt stage
~~~

Execute

### See what options are supported
 ~~~bash
 ./extr --help
 ~~~


### Export to JSON
~~~bash
./extr -m export --hosts localhost -k explore_main_0001 -t contentcontainers3 --progress > ouput
~~~

### Import
~~~bash
cat ouput | ./extr -m import --hosts localhost -k explore_main_0001 -t contentcontainers4 --progress
~~~

### Export and Import
~~~bash
./extr -m export --hosts localhost -k explore_main_0001 -t contentcontainers3 | ./extr  -m import --hosts localhost -k explore_main_0001 -t contentcontainers4 --progress
~~~
