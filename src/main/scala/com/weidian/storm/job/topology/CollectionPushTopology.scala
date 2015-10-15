package com.weidian.storm.job.topology

import com.weidian.storm.job.bolt.push._


import backtype.storm.{StormSubmitter, Config, LocalCluster}
import backtype.storm.spout.SchemeAsMultiScheme
import backtype.storm.topology.TopologyBuilder
import backtype.storm.utils.Utils

import storm.kafka.trident.GlobalPartitionInformation
import storm.kafka._
import backtype.storm.tuple.Fields
import org.apache.zookeeper.client.ConnectStringParser;
import java.util.ArrayList

object CollectionPushTopology {
  def main(args: Array[String]) {
    val zkConnectStr:String = "10.1.24.110:181"
  
    val connectStringParser = new ConnectStringParser(zkConnectStr);
    val serverInetAddresses = connectStringParser.getServerAddresses();
    val serverAddresses = new ArrayList[String](serverInetAddresses.size());
    val zkPort = serverInetAddresses.get(0).getPort();
    serverAddresses.add(serverInetAddresses.get(0).getHostName)


    val zkHosts = new ZkHosts(zkConnectStr);
    zkHosts.brokerZkPath = "" + zkHosts.brokerZkPath;

    val spoutConfig = new SpoutConfig(zkHosts, "app_orfmvsubb0e9d2e2fe59_koudai_aproxy", "", "collect_push_storm");

    spoutConfig.zkServers = serverAddresses;
    spoutConfig.zkPort = zkPort;
    spoutConfig.zkRoot = "";

    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

    val builder:TopologyBuilder  = new TopologyBuilder();
    builder.setSpout("spout", new KafkaSpout(spoutConfig), 8);
    
    builder.setBolt("split", new ParseCollectLogBolt(),8).shuffleGrouping("spout")

    val config:Config  = new Config();
    config.setDebug(true)

    if (args != null && args.length > 0) {
      config.setNumWorkers(3)
      StormSubmitter.submitTopology(args(0), config, builder.createTopology())
    } else {

      val cluster: LocalCluster = new LocalCluster()
      cluster.submitTopology("CollectionPushTopology", config, builder.createTopology())
      Utils.sleep(500000000)

      cluster.killTopology("CollectionPushTopology")
      cluster.shutdown()
    }    
  }
}
