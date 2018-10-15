package example

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, TokenAwarePolicy}

object Cassandra {

  def apply(hosts: List[String],
            keyspace: String,
            port: Int,
            dc: Option[String] = None): (Cluster, Session) = {

    val clusterBuilder = new Cluster.Builder()
      .addContactPoints(hosts: _*)
      .withPort(port)
      .withSocketOptions(new SocketOptions().setKeepAlive(true).setReadTimeoutMillis(20000))
      .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM))

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

    val cluster = dcBuilder.build
    (cluster, cluster.connect)
  }
}