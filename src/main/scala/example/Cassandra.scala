package example

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, TokenAwarePolicy}
import scala.concurrent.duration._
object Cassandra {

  def apply(hosts: List[String],
            keyspace: String,
            port: Int,
            dc: Option[String] = None): Session = {

    val clusterBuilder = new Cluster.Builder()
      .addContactPoints(hosts: _*)
      .withCompression(ProtocolOptions.Compression.LZ4)
      .withPort(port)
      .withSocketOptions(new SocketOptions().setKeepAlive(true).setReadTimeoutMillis(1.minutes.toMillis.toInt))
      .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE).setFetchSize(5000))

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