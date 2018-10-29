package example

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, TokenAwarePolicy}
import scala.concurrent.duration._
object Cassandra {

  def apply(hosts: List[String],
            keyspace: String,
            port: Int,
            consistencyLevel: ConsistencyLevel,
            fetchSize: Int,
            dc: Option[String] = None): Session = {

    val clusterBuilder = new Cluster.Builder()
      .addContactPoints(hosts: _*)
      .withCompression(ProtocolOptions.Compression.LZ4)
      .withPort(port)
      .withSocketOptions(new SocketOptions().setKeepAlive(true).setReadTimeoutMillis(1.minutes.toMillis.toInt))
      .withQueryOptions(new QueryOptions().setConsistencyLevel(consistencyLevel).setFetchSize(fetchSize))

    val dcBuilder = dc match {
      case Some(dcName) =>
        clusterBuilder
          .withLoadBalancingPolicy(
            new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder()
              .withLocalDc(dcName)
              .withUsedHostsPerRemoteDc(0).build()
            ))
      case _ => clusterBuilder
    }

    dcBuilder.build.connect
  }
}