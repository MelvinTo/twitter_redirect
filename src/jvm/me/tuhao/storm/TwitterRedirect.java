package me.tuhao.storm;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

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
import backtype.storm.task.ShellBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import weibo4j.Oauth;
import weibo4j.model.WeiboException;
import weibo4j.util.BareBonesBrowserLaunch;

public class TwitterRedirect {

	public static class TwitterMessageSpout extends BaseRichSpout {
	    SpoutOutputCollector _collector;
	    Date _startDate;
	    static int _interval = 5 * 60 * 1000; // 5 minutes per query
	    
	    static String _consumerToken = "kiD2bfu2gTWJkhhPlasfw";
	    static String _consumerSecret = "6lhg9lJNPmN7Ko8TSC7u10aCh0ueZIPiQcsb7KqfGRE";
//	    static String _accessToken = "79633-6MAgVoy0klpOgiEdAjPPRipsgtBqx05U7ub2MMYAg";
//	    static String _accessTokenSecret = "VNvTqpu1v32sK3J7QmKLSPjcAXIP9zWZmzMWQKnWY";
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
//	 	        RequestToken requestToken = _twitter.getOAuthRequestToken();
	            _twitter.setOAuthAccessToken(new AccessToken(_accessToken, _accessTokenSecret));

//	 	        AccessToken accessToken = null;
//	 	        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
//	 	        while (null == accessToken) {
//	 	          System.out.println("Open the following URL and grant access to your account:");
//	 	          System.out.println(requestToken.getAuthorizationURL());
//	 	          System.out.print("Enter the PIN(if aviailable) or just hit enter.[PIN]:");
//	 	          String pin = br.readLine();
//	 	          try{
//	 	             if(pin.length() > 0){
//	 	               accessToken = _twitter.getOAuthAccessToken(requestToken, pin);
//	 	             }else{
//	 	               accessToken = _twitter.getOAuthAccessToken();
//	 	             }
//	 	             System.out.println("Access token is '" + accessToken.getToken() + "', and secret is '" + accessToken.getTokenSecret() + "'");
//	 	             
//	 	          } catch (TwitterException te) {
//	 	            if(401 == te.getStatusCode()){
//	 	              System.out.println("Unable to get the access token.");
//	 	            }else{
//	 	              te.printStackTrace();
//	 	            }
//	 	          }
//	 	        }
	        } catch (Exception e) {
	        	e.printStackTrace();
	        }
	       
	        
	    }

	    @Override
	    public void nextTuple() {
	    	System.out.println("next Tuple");
	    	_collector.emit(new Values("this is a test message http://t.co/re0NZXMm"));
	    	
	    	if(true) {
	    		Utils.sleep(_interval);
	    		return;
	    	}
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
			
	    	Utils.sleep(_interval);

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
	 public static class ShortURLToLongBolt extends ShellBolt implements IRichBolt {

	    public ShortURLToLongBolt() {
	        super("ruby", "shorturltolong.rb");
	    }
	        
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("message"));			
		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			// TODO Auto-generated method stub
			return null;
		}
 
	 }
	 
	 public static class LongURLToWeiboShortBolt extends BaseBasicBolt {
		 
		 private static String _accessToken = "2.00iEDJqBq4ckjCa24f38ba3dTFk1NB";
		 
		 @Override
		 public void declareOutputFields(OutputFieldsDeclarer declarer) {
			 declarer.declare(new Fields("message"));						
		 }

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			
			String message = input.getString(0);
			
			try {
				Oauth oauth = new Oauth();
				
				BareBonesBrowserLaunch.openURL(oauth.authorize("code"));
				System.out.println(oauth.authorize("code"));
				System.out.print("Hit enter when it's done.[Enter]:");
				
				BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

				String code = br.readLine();
				//			Log.logInfo("code: " + code);
				try{
					System.out.println(oauth.getAccessTokenByCode(code));
				} catch (WeiboException e) {
					if(401 == e.getStatusCode()){
						//					Log.logInfo("Unable to get the access token.");
					}else{
						e.printStackTrace();
					}
				}
			} catch (Exception e) {

			}
			
			collector.emit(new Values(message));
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
        builder.setBolt("short2long", new ShortURLToLongBolt(), 2).shuffleGrouping("twitter");
        builder.setBolt("long2short", new LongURLToWeiboShortBolt(), 2).shuffleGrouping("short2long");
        builder.setBolt("weibo", new WeiboPostBolt(), 1)
                 .shuffleGrouping("long2short");

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
