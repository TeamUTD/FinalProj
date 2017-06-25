package com.realtime.finalproject;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

@SuppressWarnings("serial")
public class TwitterSpout extends BaseRichSpout{

	SpoutOutputCollector _spoutOutputCollector;
	private LinkedBlockingQueue<Status> queue = null;
	private TwitterStream _twitterStream;
	   
	public TwitterSpout() {
		
	}

	@Override
	public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		queue = new LinkedBlockingQueue<Status>(1000);
		_spoutOutputCollector = spoutOutputCollector;
						
		_twitterStream = new TwitterStreamFactory().getInstance();
		_twitterStream.addListener(new StatusListener(){

			@Override
			public void onException(Exception arg0) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice arg0) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void onScrubGeo(long arg0, long arg1) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void onStatus(Status status) {
				// TODO Auto-generated method stub
				queue.add(status);
			}

			@Override
			public void onTrackLimitationNotice(int arg0) {
				// TODO Auto-generated method stub
				
			}});
		
		FilterQuery query = new FilterQuery().track("Lebron James");
		_twitterStream.filter(query);
		
	}

	@Override
	public void nextTuple() {
		Status ret = queue.poll();
		
		if(ret == null)
		{
			Utils.sleep(50);
		}
		else
		{
			_spoutOutputCollector.emit(new Values(ret));
		}
		
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		super.ack(msgId);
	}

	@Override
	public void close() {
		_twitterStream.shutdown();
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		super.fail(msgId);
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("tweet"));
	}
}
