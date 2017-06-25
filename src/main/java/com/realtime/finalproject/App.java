package com.realtime.finalproject;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class App 
{
    public static void main( String[] args )
    {
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter-spout", new TwitterSpout());
        builder.setBolt("twitter-trend-bolt", new TwitterTrendBolt()).shuffleGrouping("twitter-spout");
        
        Config config = new Config();
		config.setDebug(false);
		
		// Create an instance of LocalCluster class for executing topology in local mode
		LocalCluster cluster = new LocalCluster();
		
		// TwitterRealTimeTopology is the name of submitted topology.
		cluster.submitTopology("TwitterRealTimeTopology", config, builder.createTopology());
	
		// Kill the LearningStormTopology
		//cluster.killTopology("LearningStormTopology");
		
		// Shutdown the storm test cluster
		//cluster.shutdown();
    }
}
