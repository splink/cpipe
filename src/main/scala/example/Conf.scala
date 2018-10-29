package example

import org.rogach.scallop.ScallopConf

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  version("Extr 49386")

  banner(
    """
      |Export: ./extr --mode export --hosts localhost --keyspace someKeyspace --table someTable --progress
      |Import: ./extr --mode import --hosts localhost --keyspace someKeyspace --table someTable --progress
      |""".stripMargin)

  val hosts = opt[List[String]](required = true, descr = "The cassandra ip(s). Comma-separated, if there is more then one.")
  val keyspace = opt[String](required = true, descr = "The name of the keyspace.")
  val table = opt[String](required = true, descr = "The name of the table.")
  val port = opt[Int](default = Some(9042), descr = "Optional, the port, default value is 9042.")
  val progress = opt[Boolean](default = Some(false), descr = "Print the progress to stderr.")
  val fetchSize = opt[Int](default = Some(5000), descr = "The amount of rows which is retrieved simultaneously. Defaults to 5000.")
  val consistencyLevel = choice(
    choices = Seq("ANY", "ONE", "TWO", "THREE", "QUORUM", "ALL", "LOCAL_QUORUM", "EACH_QUORUM",
      "SERIAL", "LOCAL_SERIAL", "LOCAL_ONE"), default = Some("LOCAL_QUORUM"),
    descr = "The Consistency level. Defaults to LOCAL_QUORUM.")
  val mode = choice(choices = Seq("import", "export"), required = true, descr = "Select the mode.")
  val query = opt[String](default = Some(""), descr = "A custom export query filter/ordering/limit. For instance: 'where x in (1,2,3) limit 5'")

  validateOpt (mode, query) {
    case(Some(m), Some(q)) if m == "import" && q.nonEmpty => Left("A query can only be used in export mode.")
    case _ => Right(Unit)
  }

  verify()
}