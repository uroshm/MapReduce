package com.mapreduce.app;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import com.mapreduce.app.data.Tweet;
import com.mapreduce.app.service.Orchestrator;

@SpringBootTest
class AppApplicationTests {

	@Autowired
	private Orchestrator orchestrator;

	@Test
	void createTweets() throws Exception {
		var tweets = generateTweets();
		orchestrator.initializeMappers(tweets, 4);
		orchestrator.initializeReducers(2);
	}

	private List<Tweet> generateTweets() {

		Map<String, Integer> hashtags = Map.of(
				"#Basketball", 500,
				"#Soccer", 5000,
				"#Football", 500,
				"#Boxing", 250,
				"#MMA", 250);

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
