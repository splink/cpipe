package example

import com.datastax.driver.core.ConsistencyLevel

final case class Config(mode: String, connection: Connection, selection: Selection, credentials: Credentials, flags: Flags, settings: Settings)

case object Config {
  def fromArguments(args: Arguments) =
    for {
      hosts <- args.hosts.toOption
      keyspace <- args.keyspace.toOption
      table <- args.table.toOption
      filter <- args.filter.toOption.map(_.mkString(" "))
      port <- args.port.toOption
      username <- args.username.toOption
      password <- args.password.toOption
      beQuiet <- args.quiet.toOption
      threads <- args.threads.toOption
      fetchSize <- args.fetchSize.toOption
      useCompression <- args.compression.toOption.map {
        case c if c == "ON" => true
        case _ => false
      }
      mode <- args.mode.toOption
      consistencyLevel <- args.consistencyLevel.toOption.map {
        case cl if cl == ConsistencyLevel.ANY.name() => ConsistencyLevel.ANY
        case cl if cl == ConsistencyLevel.ONE.name() => ConsistencyLevel.ONE
        case cl if cl == ConsistencyLevel.TWO.name() => ConsistencyLevel.TWO
        case cl if cl == ConsistencyLevel.THREE.name() => ConsistencyLevel.THREE
        case cl if cl == ConsistencyLevel.QUORUM.name() => ConsistencyLevel.QUORUM
        case cl if cl == ConsistencyLevel.ALL.name() => ConsistencyLevel.ALL
        case cl if cl == ConsistencyLevel.LOCAL_QUORUM.name() => ConsistencyLevel.LOCAL_QUORUM
        case cl if cl == ConsistencyLevel.EACH_QUORUM.name() => ConsistencyLevel.EACH_QUORUM
        case cl if cl == ConsistencyLevel.SERIAL.name() => ConsistencyLevel.SERIAL
        case cl if cl == ConsistencyLevel.LOCAL_SERIAL.name() => ConsistencyLevel.LOCAL_SERIAL
        case cl if cl == ConsistencyLevel.LOCAL_ONE.name() => ConsistencyLevel.LOCAL_ONE
      }
    } yield {
      Config(mode,
        Connection(hosts, port),
        Selection(keyspace, table, filter),
        Credentials(username, password),
        Flags(!beQuiet, useCompression),
        Settings(fetchSize, consistencyLevel, threads))
    }
}

final case class Connection(hosts: List[String], port: Int)

final case class Selection(keyspace: String, table: String, filter: String)

final case class Credentials(username: String, password: String)

final case class Flags(showProgress: Boolean, useCompression: Boolean)

final case class Settings(fetchSize: Int, consistencyLevel: ConsistencyLevel, threads: Int)
