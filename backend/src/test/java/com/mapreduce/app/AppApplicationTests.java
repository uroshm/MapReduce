package com.mapreduce.app;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import com.mapreduce.app.data.PartitionStrategy;
import com.mapreduce.app.data.Tweet;
import com.mapreduce.app.service.Orchestrator;

@SpringBootTest
class AppApplicationTests {

	@Autowired
	private Orchestrator orchestrator;

	@Test
	void partitionNaiveHash() throws Exception {
		var tweets = generateTweets();
		orchestrator.initializeMappers(tweets, 4);
		orchestrator.initializeReducers(2);
		orchestrator.runMapReduce(PartitionStrategy.NAIVE);
		Thread.sleep(8000);
		System.out.println(orchestrator.collectResults());
	}

	@Test
	void partitionSmart() throws Exception {
		var tweets = generateTweets();
		orchestrator.initializeMappers(tweets, 4);
		orchestrator.initializeReducers(2);
		orchestrator.runMapReduce(PartitionStrategy.EQUALLY_WEIGHTED);
		Thread.sleep(8000);
		System.out.println(orchestrator.collectResults());
	}

	private List<Tweet> generateTweets() {
		Map<String, Integer> hashtags = Map.of(
				"#Basketball", 200,
				"#Soccer", 4000,
				"#Football", 100,
				"#Racquetball", 200,
				"#Pickleball", 300,
				"#Boxing", 100);

		ArrayList<Tweet> tweets = new ArrayList<>();
		for (var entry : hashtags.entrySet()) {
			int count = entry.getValue();
			for (int i = 0; i < count; i++) {
				var tweet = new Tweet();
				tweet.setHashtag(entry.getKey());
				tweet.setMessage("I love this sport!");
				tweets.add(tweet);
			}
		}
		Collections.shuffle(tweets);
		return tweets;
	}
}
