package emergingtopicdetector;

import java.io.*;
import java.util.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.TwitterUtils;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import twitter4j.HashtagEntity;
import twitter4j.Status;

public class EmergingTopicDetector {
	static long counter = 1;
	protected static StanfordCoreNLP pipeline = new StanfordCoreNLP(getProperties());
	public static void main(String[] args) throws IOException {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		BufferedWriter bw=new BufferedWriter(new FileWriter("outputFile.txt"));
		
		// Parse input arguments for accessing twitter streaming api
		if(args.length<4)
		{
			System.err.println("Usage: Demo <consumer key> <consumer secret> " +"<access token> <access token secret>");
			System.exit(1);
		}
		String consumerKey = args[0];
		String consumerSecret = args[1];
		String accessToken = args[2];
		String accessTokenSecret = args[3];
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(
				"TopicDetector");
		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				Durations.seconds(10));

		//Get Twitter stream 
		JavaReceiverInputDStream<Status> stream = getTwitterStream(jssc,
				consumerKey, consumerSecret, accessToken, accessTokenSecret);
		//Print twitter stream
		stream.print();
		Map<String, List<Status>> hm_old = new HashMap<>();
		final List<Map<String, List<Status>>> mapList = new ArrayList<>();
		mapList.add(hm_old);
		
		//Find emerging topic and perform sentiment analysis on the tweets of these emerging topics
		stream.foreachRDD(rdd -> {
			Map<String, List<Status>> new_map = new HashMap<>();
			mapList.add(new_map);

			for (Status st : rdd.collect()) {
				for (HashtagEntity entity : st.getHashtagEntities()) {
					List<Status> list;
					String entityName = entity.getText();
					if (!new_map.containsKey(entityName)) {
						list = new ArrayList<>();
						new_map.put(entityName, list);
					}
					list = new_map.get(entityName);
					list.add(st);
				}

			}

			// ToDo: Do Analysis on Old and New Map
			analyze(mapList, bw);

			// ToDo: Remove old hashmap from mapList
			mapList.remove(0);

		});

		System.out.println();
		// Start streaming
		jssc.start();
		// Wait for termination
		jssc.awaitTermination();
		//Close buffered writer
		bw.close();

	}

	private static JavaReceiverInputDStream<Status> getTwitterStream(
			JavaStreamingContext jssc, String consumerKey,
			String consumerSecret, String accessToken,
			String accessTokenSecret) {
		
		//Set the system properties so that Twitter4j library used by twitter stream can use them to generate OAuth credentials
		System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
		System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
		System.setProperty("twitter4j.oauth.accessToken", accessToken);
		System.setProperty("twitter4j.oauth.accessTokenSecret",
				accessTokenSecret);

		// creating twitter input stream
		return TwitterUtils.createStream(jssc);
	}

	//Do analysis on old and new hashmap in the mapList
	private static void analyze(List<Map<String, List<Status>>> mapList, BufferedWriter bw) throws IOException {

		// ToDo: Find Emerging topics
		List<String> emergingTopicList = findEmergingTopics(mapList.get(0),
				mapList.get(1));
		// ToDo: Do Sentiment analysis of tweets of emerging topics
		doSentimentAnalysis(mapList.get(1), emergingTopicList, bw);

	}

	//Find emerging topic using old and new hashmap
	private static List<String> findEmergingTopics(
			Map<String, List<Status>> oldMap, Map<String, List<Status>> newMap) {
		int maxDiff = 0;
		List<String> emergingTopicList = new ArrayList<String>();

		if (newMap == null || newMap.isEmpty())
			return emergingTopicList;

		for (Map.Entry<String, List<Status>> entry : newMap.entrySet()) {
			int countInNewMap = entry.getValue().size();
			int countInOldMap = oldMap.containsKey(entry.getKey()) ? oldMap
					.get(entry.getKey()).size() : 0;
			int currDiff = countInNewMap - countInOldMap;
			if (currDiff < maxDiff)
				continue;
			if (currDiff > maxDiff) {
				maxDiff = currDiff;
				emergingTopicList = new ArrayList<String>();
			}
			emergingTopicList.add(entry.getKey());
		}
		return emergingTopicList;
	}

	//Do Sentiment analysis of tweets of emerging topics
	private static void doSentimentAnalysis(Map<String, List<Status>> statusByTopic,
			List<String> emergingTopicList, BufferedWriter bw) throws IOException {
		if (emergingTopicList == null || emergingTopicList.isEmpty()
				|| statusByTopic == null || statusByTopic.isEmpty())
			return;
		
		
		StringBuffer sb = new StringBuffer();
		sb.append("\n");
		sb.append("....................................................................................................................................");
		sb.append("\n");
		sb.append("\t\t\t\t\t\t\t\tWindow :");
		sb.append(counter);
		sb.append("\n");
		sb.append("....................................................................................................................................");
		sb.append("\n");
		for (String emergingTopic : emergingTopicList) {
			if (!statusByTopic.containsKey(emergingTopic))
				continue;
			//Get all tweets for the given emerging topic
			List<Status> statusList = statusByTopic.get(emergingTopic);
			for (Status st : statusList) {
				//Ignore the tweets that are not in English
				if (!st.getLang().equals("en"))
					continue;
				//Get sentiment for this tweet
				String sentiment = getSentiment(st.getText());
				
				sb.append("\nTopic ->"+ emergingTopic);
				sb.append("  ,\t Sentiment -> "+ sentiment);
				sb.append("  ,\t Content -> " + st.getText());
				//System.out.println(sb.toString());
			}
			
		}
		sb.append("\n");
		//Write the results of sentiment analysis for this window to output file
		bw.write(sb.toString());
		bw.flush();
		counter++;

	}

	//Get sentiment for the given tweet
	private static String getSentiment(String tweet) {
		int feedBack = findSentimentScore(tweet);
		String result = null;
		switch (feedBack) {
		case 0:
			result = "VERY NEGATIVE";
			break;
		case 1:
			result = "NEGATIVE";
			break;
		case 2:
			result = "NEUTRAL";
			break;
		case 3:
			result = "POSITIVE";
			break;
		case 4:
			result = "VERY POSITIVE";
			break;
		default:
			result = "NOT UNDERSTOOD";
			break;
		}
		return result;
	}

	//Get sentiment score for the given tweet content
	private static int findSentimentScore(String tweet) {

		int mainSentiment = 0;
		if (tweet != null && tweet.length() > 0) {
			int longest = 0;
			Annotation annotation = pipeline.process(tweet);
			for (CoreMap sentence : annotation
					.get(CoreAnnotations.SentencesAnnotation.class)) {
				Tree tree = sentence
						.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
				int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
				String partText = sentence.toString();
				if (partText.length() > longest) {
					mainSentiment = sentiment;
					longest = partText.length();
				}

			}
		}
		return mainSentiment;
	}

	private static Properties getProperties() {
		Properties props = new Properties();
		props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
		return props;
	}

}
