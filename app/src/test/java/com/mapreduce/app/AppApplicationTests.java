package com.mapreduce.app;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
		orchestrator.runMappers(PartitionStrategy.NAIVE_HASH);
		orchestrator.getReducers().forEach(reducer -> {
			var thread = new Thread(reducer);
			thread.start();
		});

		Thread.sleep(5000);
		orchestrator.getReducers().forEach(reducer -> {
			reducer.getResult().forEach((key, value) -> {
				System.out.println(key + ": " + value);
			});
		});
	}

	@Test
	void partitionEquallyWeighted() throws Exception {
		var tweets = generateTweets();
		orchestrator.initializeMappers(tweets, 4);
		orchestrator.initializeReducers(2);
		orchestrator.runMappers(PartitionStrategy.EQUALLY_WEIGHTED);
		orchestrator.getReducers().forEach(reducer -> {
			var thread = new Thread(reducer);
			thread.start();
		});

		Thread.sleep(5000);
		orchestrator.getReducers().forEach(reducer -> {
			reducer.getResult().forEach((key, value) -> {
				System.out.println(key + ": " + value);
			});
		});
	}

	private List<Tweet> generateTweets() {

		Map<String, Integer> hashtags = Map.of(
				"#Basketball", 5000,
				"#Soccer", 50000000,
				"#Football", 5000,
				"#Boxing", 2500);

		ArrayList<Tweet> tweets = new ArrayList<>();
		for (var entry : hashtags.entrySet()) {
			int count = entry.getValue();
			for (int i = 0; i < count; i++) {
				var tweet = new Tweet();
				tweet.setId(UUID.randomUUID());
				tweet.setUser("user" + i % 1000);
				tweet.setHashtag(entry.getKey());
				tweet.setMessage("I love this sport!");
				tweets.add(tweet);
			}
		}
		Collections.shuffle(tweets);
		return tweets;
	}
}
