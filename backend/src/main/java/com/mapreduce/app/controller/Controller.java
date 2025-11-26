package com.mapreduce.app.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.mapreduce.app.data.PartitionStrategy;
import com.mapreduce.app.data.Tweet;
import com.mapreduce.app.service.Orchestrator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

@RestController
@RequiredArgsConstructor
@Slf4j
public class Controller {

    private final Orchestrator orchestrator;

    @PostMapping("process")
    public String processTweets(@RequestBody Map<String, Integer> hashtagCounts,
            @RequestParam int numberOfMappers,
            @RequestParam int numberOfReducers,
            @RequestParam String partitionStrategy) {
        processAsync(hashtagCounts, numberOfMappers, numberOfReducers, partitionStrategy);
        return "Processing started. Use /getStatus to check progress and /getResults to retrieve results.";
    }

    @Async
    public void processAsync(Map<String, Integer> hashtagCounts, int numberOfMappers, 
            int numberOfReducers, String partitionStrategy) {
        try {
            orchestrator.reset();
            orchestrator.initializeMappers(getTweetsFromHashtagCounts(hashtagCounts), numberOfMappers);
            orchestrator.initializeReducers(numberOfReducers);
            orchestrator.runMapReduce(Enum.valueOf(PartitionStrategy.class, partitionStrategy));
            log.info("MapReduce processing completed successfully");
        } catch (Exception e) {
            log.error("Error during MapReduce processing: ", e);
        }
    }

    List<Tweet> getTweetsFromHashtagCounts(Map<String, Integer> hashtagCounts) {
        var tweets = new ArrayList<Tweet>();
        hashtagCounts.forEach((hashtag, count) -> {
            for (int i = 0; i < count; i++) {
                var tweet = new Tweet();
                tweet.setHashtag(hashtag);
                tweet.setMessage("This is my favorite sport! " + hashtag);
                tweets.add(tweet);
            }
        });
        return tweets;
    }

    @GetMapping("getStatus")
    public String getStatus() {
        var builder = new StringBuilder();
        var reducers = orchestrator.getReducers();
        if (reducers == null || reducers.isEmpty()) {
            return "No processing task running or completed yet.";
        }
        reducers.forEach(r -> {
            builder.append(r.getName())
                    .append(" processed ")
                    .append(r.getRecordsProcessed())
                    .append(" items.\n");
        });
        return builder.toString();
    }

    @GetMapping("getResults")
    public String getResults() {
        try {
            var finalResults = orchestrator.collectResults();
            return finalResults != null ? finalResults : "No results available yet.";
        } catch (Exception e) {
            log.error("Error collecting results: ", e);
            return "Error collecting results or processing not complete.";
        }
    }

    @PostMapping("reset")
    public HttpStatus reset() {
        orchestrator.reset();
        return HttpStatus.OK;
    }

}
