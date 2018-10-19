package example

import org.rogach.scallop.ScallopConf

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val hosts = opt[List[String]](required = true, descr = "The cassandra database ip(s). Comma-separated, if there is more then one.")
  val keyspace = opt[String](required = true, descr = "The name of the keyspace")
  val table = opt[String](required = true, descr = "The name of the table")
  val port = opt[Int](default = Some(9042), descr = "Optional, the port, default value is 9042")
  val progress = opt[Boolean](default = Some(false), descr = "Print the progress to stderr, defaults to false")
  val fetchSize = opt[Int](default = Some(3000), descr = "The amount of rows which is retrieved simultaneously. Defaults to 3000")

  verify()
}