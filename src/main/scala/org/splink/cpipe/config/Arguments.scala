package org.splink.cpipe.config

import org.rogach.scallop.ScallopConf
class Arguments(arguments: Seq[String]) extends ScallopConf(arguments) {
  version(s"${BuildInfo.name} ${BuildInfo.version} ${BuildInfo.commitId}")

  banner(
    s"""
      |Import/Export rows from a Cassandra database
      |
      |Export
      |* export all rows from a table
      |./cpipe --mode export2 --hosts localhost --keyspace someKeyspace --table someTable
      |
      |* export only rows which pass a filter
      |./cpipe --mode export --hosts localhost --keyspace someKeyspace --table someTable --filter "limit 10"
      |
      |Import
      |* import from a file
      |cat some.json | ./cpipe --mode import --hosts localhost --keyspace someKeyspace --table someTable
      |
      |* import an export
      |./cpipe --mode export --hosts remotehost --keyspace someKeyspace --table someTable --quiet | ./cpipe --mode import --hosts localhost --keyspace anotherKeyspace --table someTable
      |
      |""".stripMargin)

  val hosts = opt[List[String]](required = true,
    descr = "The cassandra host ip(s). Comma-separated, if there is more then one.")

  val keyspace = opt[String](required = true,
    descr = "The name of the keyspace.")

  val table = opt[String](required = true,
    descr = "The name of the table.")

  val filter = opt[List[String]](default = Some(Nil),
    descr = "A custom filter to filter, order or limit the returned rows. For instance: 'where x = 'a' limit 5'")

  val username = opt[String](required = false, default = Some(""),
    descr = "The username for the cassandra cluster, if PasswordAuthenticator is used.")

  val password = opt[String](required = false, default = Some(""),
    descr = "The password for the cassandra cluster, if PasswordAuthenticator is used.")

  val port = opt[Int](default = Some(9042),
    descr = "Optional, the port, default value is 9042.")

  val quiet = opt[Boolean](default = Some(false),
    descr = "Do not print the progress to stderr.")

  val verbose = opt[Boolean](default = Some(false),
    descr = "Do print additional information to stderr.")

  val fetchSize = opt[Int](default = Some(5000),
    descr = "The amount of rows which is retrieved simultaneously. Defaults to 5000.")

  val batchSize = opt[Int](default = Some(500),
    descr = "The amount of rows which is saved simultaneously when using mode import2. Defaults to 500.")

  val threads = opt[Int](default = Some(32),
    descr = "The amount of parallelism used in export2 mode. Defaults to 32 parallel requests.")

  val consistencyLevel = choice(
    choices = Seq("ANY", "ONE", "TWO", "THREE", "QUORUM", "ALL", "LOCAL_QUORUM", "EACH_QUORUM",
      "SERIAL", "LOCAL_SERIAL", "LOCAL_ONE"), default = Some("LOCAL_QUORUM"),
    descr = "The Consistency level. Defaults to LOCAL_QUORUM.")

  val compression = choice(Seq("ON", "OFF"), default = Some("ON"),
    descr = "Use LZ4 compression and trade reduced network traffic for CPU cycles. Defaults to ON")

  val mode = choice(choices = Seq("import", "import2", "export", "export2"), required = true,
    descr = "Select the mode. Choose mode 'import' to import data. " +
      "Choose mode 'import2' to import data with a prepared statement (faster, but only for tables with fixed columns); " +
      "Choose mode 'export' to export data (optional with a filter); " +
      "Choose mode 'export2' to export data using token ranges to increase performance and reduce load on the cluster. " +
      "'export2' mode cannot be combined with a filter and it requires that the cluster uses Murmur3Partitioner. " +
      "'export2' works best, if the table rows are well partitioned around the cluster.")

  validateOpt (mode, filter) {
    case(Some(m), Some(f)) if m == "import" && f.nonEmpty => Left("A filter can not be used in import mode.")
    case(Some(m), Some(f)) if m == "export2" && f.nonEmpty => Left("A filter can not be used in export2 mode.")
    case _ => Right(Unit)
  }

  verify()
}
