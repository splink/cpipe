package example

import org.rogach.scallop.ScallopConf

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  version("Extr 49386")

  banner(
    """
      |Import/export rows from a Cassandra database
      |
      |Export:
      |* export all rows from a table
      |./extr --mode export --hosts localhost --keyspace someKeyspace --table someTable
      |
      |* export only rows which pass a filter
      |./extr --mode export --hosts localhost --keyspace someKeyspace --table someTable --filter "limit 10"
      |
      |* export all rows from a table using token ranges which is faster for tables > 10000 rows
      |./extr --mode export2 --hosts localhost --keyspace someKeyspace --table someTable
      |
      |Import
      |* import from a file:
      |cat some.json | ./extr --mode import --hosts localhost --keyspace someKeyspace --table someTable
      |
      |* import an export
      |./extr --mode export --hosts remotehost --keyspace someKeyspace --table someTable --quiet | ./extr --mode import --hosts localhost --keyspace anotherKeyspace --table someTable
      |
      |""".stripMargin)

  val hosts = opt[List[String]](required = true,
    descr = "The cassandra ip(s). Comma-separated, if there is more then one.")

  val keyspace = opt[String](required = true,
    descr = "The name of the keyspace.")

  val table = opt[String](required = true,
    descr = "The name of the table.")

  val filter = opt[List[String]](default = Some(Nil),
    descr = "A custom filter to filter, order or limit the returned rows. For instance: 'where x in (1,2,3) limit 5'")

  val username = opt[String](required = false, default = Some(""),
    descr = "The username for the cassandra cluster, if PasswordAuthenticator is used.")

  val password = opt[String](required = false, default = Some(""),
    descr = "The password for the cassandra cluster, if PasswordAuthenticator is used.")

  val port = opt[Int](default = Some(9042),
    descr = "Optional, the port, default value is 9042.")

  val quiet = opt[Boolean](default = Some(false),
    descr = "Do not print the progress to stderr.")

  val fetchSize = opt[Int](default = Some(5000),
    descr = "The amount of rows which is retrieved simultaneously. Defaults to 5000.")

  val threads = opt[Int](default = Some(8),
    descr = "The amount of parallelism during used in export2 mode")

  val consistencyLevel = choice(
    choices = Seq("ANY", "ONE", "TWO", "THREE", "QUORUM", "ALL", "LOCAL_QUORUM", "EACH_QUORUM",
      "SERIAL", "LOCAL_SERIAL", "LOCAL_ONE"), default = Some("LOCAL_QUORUM"),
    descr = "The Consistency level. Defaults to LOCAL_QUORUM.")

  val compression = choice(Seq("ON", "OFF"), default = Some("ON"),
    descr = "Use LZ4 compression and trade reduced network traffic for CPU cycles. Defaults to ON")

  val mode = choice(choices = Seq("import", "export", "export2"), required = true,
    descr = "Select the mode. Choose mode 'import' to import data. " +
      "Choose mode 'export' to export data (optional with a filter); " +
      "Choose mode 'export2' to export data using token ranges to increase performance and reduce load on the cluster. " +
      "'export2' mode cannot be combined with a filter. 'export2' is NOT faster if the tables are small (< 10000 rows) ")

  validateOpt (mode, filter) {
    case(Some(m), Some(f)) if m == "import" && f.nonEmpty => Left("A filter can not be used in import mode.")
    case(Some(m), Some(f)) if m == "export2" && f.nonEmpty => Left("A filter can not be used in export2 mode.")
    case _ => Right(Unit)
  }

  verify()
}