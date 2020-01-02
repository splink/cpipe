# CPipe - A JSON Import/Export tool for Apache Cassandra

Export Cassandra tables as JSON to stdout. 
Import Cassandra tables as JSON from stdin.

Using stdin/stdout opens the door to the land of pipes and thus enables to slice and dice the stream of JSON data using command line tools. Most useful for the processing of streamed json data will probably be the infamous [jq](https://github.com/stedolan/jq).


###### Download the [current release]
as Zip: https://github.com/splink/cpipe/releases/download/v0.0.2/cpipe-0.0.2.zip
~~~bash
unzip cpipe-0.0.2.zip
./bin/cpipe --help 
~~~

as Rpm: https://github.com/splink/cpipe/releases/download/v0.0.2/cpipe-0.0.2.noarch.rpm
~~~bash
rpm -i cpipe-0.0.2.noarch.rpm
cpipe --help
~~~

as deb: https://github.com/splink/cpipe/releases/download/v0.0.2/cpipe-0.0.2_all.deb
 ~~~bash
dpkg -i cpipe-0.0.2.noarch.rpm
cpipe --help
 ~~~


[![asciicast](https://asciinema.org/a/XLXvSasorNPkMHH5isH5U0KKq.svg)](https://asciinema.org/a/XLXvSasorNPkMHH5isH5U0KKq?autoplay=1&loop=1)

Build from source with [SBT](https://www.scala-sbt.org):
~~~bash
sbt stage
~~~

## Recipes

##### Import from JSON
~~~bash
cat assets.json | ./cpipe --mode import --hosts localhost --keyspace space --table assets
~~~

##### Export to JSON
Use the export2 mode which leverages optimized range queries to speed up the retrieval of data and reduce the work the coordinator node has to perform.
If you don't need to filter and the table is well partitioned, 'export2' should be your first choice for export.
~~~bash
./cpipe --mode export2 --hosts localhost --keyspace space --table assets > assets.json
~~~


##### Export just a subset of data
Sometimes it is not feasible to import a complete table, then it's time to use a filter.
~~~bash
./cpipe --mode export --hosts localhost --keyspace space --table assets --filter "limit 100" > ouput
~~~

##### Export and Import
During development it is useful to quickly import tables from another Cassandra into the local development environment
~~~bash
./cpipe --mode export2 --hosts localhost --keyspace space --table assets --quiet | ./cpipe --mode import --hosts otherhost --keyspace space --table assets
~~~

##### Export, filter using jq, then Import
Piping into jq to transform or filter the data before importing is often useful.
~~~bash
./cpipe --mode export2 --hosts localhost --keyspace space --table assets --quiet | jq 'select(.name == "" | not)' | ./cpipe --mode import --hosts otherhost --keyspace space --table assets
~~~


##### Import several tables from another Cassandra database (into a local database)
If you want to process multiple tables, a simple for loop does the trick.
~~~bash
#!/bin/sh
src_host="somehost"
target_host="localhost"
src_keyspace="space"
target_keyspace="space"
tables=(assets products campaigns)

for table in ${tables[@]}; do
    echo "import $table"
    cpipe --mode export2 --hosts src_host --keyspace $src_keyspace --table $table --quiet | cpipe --mode import --hosts $target_host --keyspace $target_keyspace --table $table
done
~~~


##### See what options are supported
Of course there are tons of other useful options, all ready to be used. List them with '--help'
 ~~~bash
 ./cpipe --help
 ~~~
