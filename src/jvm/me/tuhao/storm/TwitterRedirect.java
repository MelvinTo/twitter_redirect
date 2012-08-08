package me.tuhao.storm;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;

import storm.starter.WordCountTopology.SplitSentence;
import storm.starter.WordCountTopology.WordCount;
import storm.starter.spout.RandomSentenceSpout;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;
import twitter4j.auth.RequestToken;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TwitterRedirect {

	public static class TwitterMessageSpout extends BaseRichSpout {
	    SpoutOutputCollector _collector;
	    Date _startDate;
	    static int _interval = 1000;
	    static String _consumerToken = "kiD2bfu2gTWJkhhPlasfw";
	    static String _consumerSecret = "6lhg9lJNPmN7Ko8TSC7u10aCh0ueZIPiQcsb7KqfGRE";
	    static String _accessToken = "79633-6MAgVoy0klpOgiEdAjPPRipsgtBqx05U7ub2MMYAg";
	    static String _accessTokenSecret = "VNvTqpu1v32sK3J7QmKLSPjcAXIP9zWZmzMWQKnWY";
	    static Twitter _twitter;
	  
	    @Override
	    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	        _collector = collector;
	        //_startDate = new Date();
	        Calendar c = Calendar.getInstance();
	        c.set(2012,7,2);
	        _startDate = c.getTime();
	        
	        try {
	        	 _twitter = new TwitterFactory().getInstance();
	 	        
	 	        _twitter.setOAuthConsumer(_consumerToken, _consumerSecret);
	 	        RequestToken requestToken = _twitter.getOAuthRequestToken();
	 	        AccessToken accessToken = null;
	 	        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
	 	        while (null == accessToken) {
	 	          System.out.println("Open the following URL and grant access to your account:");
	 	          System.out.println(requestToken.getAuthorizationURL());
	 	          System.out.print("Enter the PIN(if aviailable) or just hit enter.[PIN]:");
	 	          String pin = br.readLine();
	 	          try{
	 	             if(pin.length() > 0){
	 	               accessToken = _twitter.getOAuthAccessToken(requestToken, pin);
	 	             }else{
	 	               accessToken = _twitter.getOAuthAccessToken();
	 	             }
	 	             System.out.println("Access token is '" + accessToken.getToken() + "', and secret is '" + accessToken.getTokenSecret() + "'");
	 	             
	 	          } catch (TwitterException te) {
	 	            if(401 == te.getStatusCode()){
	 	              System.out.println("Unable to get the access token.");
	 	            }else{
	 	              te.printStackTrace();
	 	            }
	 	          }
	 	        }
	        } catch (Exception e) {
	        	e.printStackTrace();
	        }
	       
	        
	    }

	    @Override
	    public void nextTuple() {
	    	Utils.sleep(_interval);
	    	List<Status> statuses;
			try {
//				Twitter twitter = new TwitterFactory().getInstance();
//				twitter.setOAuthAccessToken(new AccessToken(_accessToken, _accessToken));
				
				Date checkDate = new Date();
				statuses = _twitter.getUserTimeline("melvinto");
				
				for(Status status : statuses) {
		    		Date createDate = status.getCreatedAt();
		    		if(createDate.before(_startDate)) {
		    			// ignore old messages
		    		} else {
		    			String message = status.getText();
		    			_collector.emit(new Values(message));
		    		}
		    	}
				
				_startDate = checkDate; // update start date to last check date 
				
			} catch (TwitterException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }        

	    @Override
	    public void ack(Object id) {
	    }

	    @Override
	    public void fail(Object id) {
	    }

	    @Override
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	        declarer.declare(new Fields("message"));
	    }
	    
	}
	
	 public static class WeiboPostBolt extends BaseBasicBolt {

		 @Override
		 public void execute(Tuple tuple, BasicOutputCollector collector) {
			 String message = tuple.getString(0);
			 System.out.println("message '" + message + "' is posted to sina");
		 }

		 @Override
		 public void declareOutputFields(OutputFieldsDeclarer declarer) {
			 declarer.declare(new Fields("message"));
		 }
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("twitter", new TwitterMessageSpout(), 1);
        
        builder.setBolt("weibo", new WeiboPostBolt(), 1)
                 .shuffleGrouping("twitter");

        Config conf = new Config();
        conf.setDebug(true);

        
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);
            
            try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        } else {        
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("twitter_message_spout", conf, builder.createTopology());
        


//            cluster.shutdown();
        }
	}

}
