package com.realtime.finalproject;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import twitter4j.Status;

public class TwitterTrendBolt extends BaseRichBolt {

	private static final long serialVersionUID = 5151173513759399636L;
	private OutputCollector _outputCollector;
	private static final Logger _logger = LoggerFactory.getLogger(TwitterTrendBolt.class);
	
	@Override
	public void execute(Tuple input) {
		// Fetch the field "site" from input tuple
		Status tweet = (Status) input.getValueByField("tweet");
		
		// Print the value of field "site" on console
		_logger.info(tweet.getText());
		
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		_outputCollector = outputCollector;
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		// TODO Auto-generated method stub
		
	}

}
