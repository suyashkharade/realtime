package com.sdk.demo;


import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SplitBolt extends BaseRichBolt {
    static Logger LOG = Logger.getLogger(SplitBolt.class);
    OutputCollector outputCollector;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        LOG.info ("prepare splitBolt");
        this.outputCollector=outputCollector;
    }

    public void execute(Tuple tuple) {
        LOG.info ("execute splitBolt");
        String line = tuple.getString(0);
        for(String word:line.split(" ")) {
            LOG.info("word "+word);
            System.out.println("word "+word);
            outputCollector.emit(new Values(word));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        System.out.print("SPlitBolt declare output");
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
