package com.sdk.demo;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SplitBolt extends BaseRichBolt {

    OutputCollector outputCollector;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        System.out.print("called prepare");
        this.outputCollector=outputCollector;
    }

    public void execute(Tuple tuple) {
        System.out.print("called execute");
        String line = tuple.getString(0);
        for(String word:line.split(" "))
            outputCollector.emit(new Values(word));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        System.out.print("called declare output");
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
