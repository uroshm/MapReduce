package com.mapreduce.app.controller;

import java.util.List;
import java.util.Map;

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
    public String processTweets(@RequestBody List<Tweet> tweets,
            @RequestParam int numberOfMappers,
            @RequestParam int numberOfReducers,
            @RequestParam String partitionStrategy) {
        try {
            orchestrator.reset();
            orchestrator.initializeMappers(tweets, numberOfMappers);
            orchestrator.initializeReducers(numberOfReducers);
            orchestrator.runMapReduce(Enum.valueOf(PartitionStrategy.class, partitionStrategy));
        } catch (Exception e) {
            log.error("Error during MapReduce processing: ", e);
            return null;
        }
        var finalResults = orchestrator.collectResults();
        return finalResults;
    }

    @GetMapping("getStatus")
    public String getStatus() {
        var builder = new StringBuilder();
        var reducers = orchestrator.getReducers();
        reducers.forEach(r -> {
            builder.append(r.getName())
                    .append(" processed ")
                    .append(r.getRecordsProcessed())
                    .append(" items.\n");
        });
        return builder.toString();
    }

    @PostMapping("reset")
    public String postMethodName(@RequestBody String entity) {
        orchestrator.reset();
        return "Reset done.";
    }

}
