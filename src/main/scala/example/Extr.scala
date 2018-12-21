package example

import example.processors.{Exporter, Exporter2, Importer}

import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

object Extr {

  def main(args: Array[String]): Unit = {
    Config.fromArguments(new Arguments(args)).foreach { config =>

      if (config.flags.showProgress) Output("Connecting to cassandra.")

      val session = createSessionFrom(config)
      session.execute(s"use ${config.selection.keyspace}")

      if (config.flags.showProgress) Output(s"Connected to cassandra ${session.getCluster.getClusterName}")

      val start = System.currentTimeMillis()

      val rowCount = Try {
        config.mode match {
          case "import" =>
            new Importer().process(session, config)
          case "export" =>
              new Exporter().process(session, config)
          case "export2" =>
            if (session.getCluster.getMetadata.getPartitioner == "org.apache.cassandra.dht.Murmur3Partitioner") {
              new Exporter2().process(session, config)
            } else {
              Console.err.println("mode 'export2' requires the cluster to use 'Murmur3Partitioner'")
            }
        }
      } match {
        case Success(count) => count
        case Failure(e) =>
          Console.err.println(
            s"\nError during '${config.mode}': message: '${if(e != null) e.getMessage else ""}'")
          System.exit(1)
          0
      }

      if (config.flags.showProgress) {
        val sec = (System.currentTimeMillis() - start) / 1000
        Console.err.println(
          s"\nProcessing $rowCount rows took ${ElapsedSecondFormat(sec)}s")
      }
    }

    System.exit(0)
  }

  def createSessionFrom(conf: Config) = Cassandra(
    conf.connection.hosts,
    conf.selection.keyspace,
    conf.connection.port,
    conf.credentials.username,
    conf.credentials.password,
    conf.settings.consistencyLevel,
    conf.settings.fetchSize,
    conf.flags.useCompression)


  object ElapsedSecondFormat {
    def zero(i: Long) = if (i < 10) s"0$i" else s"$i"

    def apply(s: Long) =
      s"""${zero(s / 3600)}:${zero((s % 3600) / 60)}:${zero(s % 60)}"""
  }

}
