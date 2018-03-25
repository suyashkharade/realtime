package com.sdk.demo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.UUID;

public class StormKafkaConnector {
    public static void main(String[] args) throws InterruptedException {
        Config config = new Config();
        config.setDebug(true);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        BrokerHosts hosts = new ZkHosts("localhost:2181");
        SpoutConfig spoutConfig = new SpoutConfig(hosts,"test","/"+"test", UUID.randomUUID().toString());
        spoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
        spoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
        System.out.println("reached till scheme");
        spoutConfig.scheme=new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        System.out.println("bulding");
        TopologyBuilder builder = new TopologyBuilder();
        System.out.println("built");
        builder.setSpout("splitspout",kafkaSpout);
        builder.setBolt("bolt",new SplitBolt()).shuffleGrouping("splitspout");
        System.out.println("initing cluster");
        LocalCluster cluster = new LocalCluster();
        System.out.println("submitting");
        cluster.submitTopology("storm cluster",config,builder.createTopology());
        System.out.println("submitted");
        Thread.sleep(100000);
        cluster.shutdown();
    }
}
