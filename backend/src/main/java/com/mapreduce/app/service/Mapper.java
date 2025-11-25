package com.mapreduce.app.service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.mapreduce.app.data.Tweet;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Data
public class Mapper implements Callable<Map<String, Integer>> {
    private final String name;
    private final List<Tweet> tweets;
    private long timeSpent;

    @Override
    public Map<String, Integer> call() throws Exception {
        long startTime = System.currentTimeMillis();
        var mapped = tweets.stream()
                .map(Tweet::getHashtag)
                .filter(hashtag -> !hashtag.isEmpty())
                .collect(
                        java.util.stream.Collectors.groupingBy(
                                hashtag -> hashtag,
                                java.util.stream.Collectors.summingInt(hashtag -> 1)));
        timeSpent = System.currentTimeMillis() - startTime;
        // System.out
        //         .println("Mapping finished by " + name + ". Processed " + tweets.size() + " tweets and took "
        //                 + timeSpent + "ms");
        return mapped;
    }
}