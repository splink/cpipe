package org.splink.cpipe

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, TokenAwarePolicy}
import scala.concurrent.duration._
object Cassandra {

  def apply(hosts: List[String],
            keyspace: String,
            port: Int,
            username: String,
            password: String,
            consistencyLevel: ConsistencyLevel,
            fetchSize: Int,
            useCompression: Boolean = true,
            dc: Option[String] = None): Session = {

    val clusterBuilder = new Cluster.Builder()
      .addContactPoints(hosts: _*)
      .withCompression(if(useCompression) ProtocolOptions.Compression.LZ4 else ProtocolOptions.Compression.NONE)
      .withPort(port)
      .withCredentials(username, password)
      .withSocketOptions(new SocketOptions().setKeepAlive(true).setReadTimeoutMillis(1.minute.toMillis.toInt))
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