package com.mapreduce.app.service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

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
        var startTime = System.currentTimeMillis();
        var mapped = tweets.stream()
                .map(Tweet::getHashtag)
                .filter(hashtag -> !hashtag.isEmpty())
                .collect(
                        Collectors.groupingBy(
                                hashtag -> hashtag,
                                Collectors.summingInt(hashtag -> 1)));
        timeSpent = System.currentTimeMillis() - startTime;
        return mapped;
    }
}