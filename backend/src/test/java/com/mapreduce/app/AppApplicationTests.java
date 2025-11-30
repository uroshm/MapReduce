package com.mapreduce.app;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
	void partitionNaiveHashHardScenario() throws Exception {
		var tweetsHardScenario = getHashtagData(hashtagsHardScenario);
		orchestrator.initializeMappers(tweetsHardScenario, 4);
		orchestrator.initializeReducers(2);
		orchestrator.runMapReduce(PartitionStrategy.NAIVE);
		Thread.sleep(15000);
		System.out.println(orchestrator.collectResults());
	}

	@Test
	void partitionNaiveHashMediumScenario() throws Exception {
		var tweetsHardScenario = getHashtagData(hashtagsMediumScenario);
		orchestrator.initializeMappers(tweetsHardScenario, 4);
		orchestrator.initializeReducers(2);
		orchestrator.runMapReduce(PartitionStrategy.NAIVE);
		Thread.sleep(10000);
		System.out.println(orchestrator.collectResults());
	}

	@Test
	void partitionNaiveHashEasyScenario() throws Exception {
		var tweetsHardScenario = getHashtagData(hashtagsEasyScenario);
		orchestrator.initializeMappers(tweetsHardScenario, 4);
		orchestrator.initializeReducers(2);
		orchestrator.runMapReduce(PartitionStrategy.NAIVE);
		Thread.sleep(10000);
		System.out.println(orchestrator.collectResults());
	}

	@Test
	void partitionSmartHard() throws Exception {
		var tweets = getHashtagData(hashtagsHardScenario);
		orchestrator.initializeMappers(tweets, 4);
		orchestrator.initializeReducers(2);
		orchestrator.runMapReduce(PartitionStrategy.SMART);
		Thread.sleep(10000);
		System.out.println(orchestrator.collectResults());
	}

	@Test
	void partitionSmartMedium() throws Exception {
		var tweets = getHashtagData(hashtagsMediumScenario);
		orchestrator.initializeMappers(tweets, 4);
		orchestrator.initializeReducers(2);
		orchestrator.runMapReduce(PartitionStrategy.SMART);
		Thread.sleep(10000);
		System.out.println(orchestrator.collectResults());
	}

	@Test
	void partitionSmartEasy() throws Exception {
		var tweets = getHashtagData(hashtagsEasyScenario);
		orchestrator.initializeMappers(tweets, 4);
		orchestrator.initializeReducers(2);
		orchestrator.runMapReduce(PartitionStrategy.SMART);
		Thread.sleep(10000);
		System.out.println(orchestrator.collectResults());
	}

	Map<String, Integer> hashtagsHardScenario = Map.of(
			"#Basketball", 200,
			"#Soccer", 4000,
			"#Football", 100,
			"#Racquetball", 200,
			"#Pickleball", 300,
			"#Boxing", 100);

	Map<String, Integer> hashtagsMediumScenario = Map.of(
			"#Basketball", 200,
			"#Soccer", 500,
			"#Football", 100,
			"#Racquetball", 200,
			"#Pickleball", 300,
			"#Boxing", 100);

	Map<String, Integer> hashtagsEasyScenario = Map.of(
			"#Basketball", 200,
			"#Soccer", 200,
			"#Football", 200,
			"#Racquetball", 200,
			"#Pickleball", 200,
			"#Boxing", 200);

	private List<Tweet> getHashtagData(Map<String, Integer> hashtags) {
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
