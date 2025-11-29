package com.mapreduce.app.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.springframework.stereotype.Service;

import com.mapreduce.app.data.PartitionStrategy;
import com.mapreduce.app.data.Tweet;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Service
@Slf4j
public class Orchestrator {
    List<Mapper> mappers = new ArrayList<>();
    List<Reducer> reducers = new ArrayList<>();
    List<BlockingQueue<Map<String, Integer>>> reducerQueues = new ArrayList<>();
    ExecutorService mapperPool = null;

    public void reset() {
        log.info("Orchestrator resetting...");
        mappers = new ArrayList<>();
        reducers = new ArrayList<>();
        reducerQueues = new ArrayList<>();
        if (mapperPool != null && !mapperPool.isShutdown()) {
            mapperPool.shutdownNow();
        }
    }

    public void initializeMappers(List<Tweet> tweets, int numberOfMappers) {
        var chunkSize = tweets.size() / numberOfMappers;
        mapperPool = Executors.newFixedThreadPool(numberOfMappers);
        mappers = new ArrayList<>();
        for (int i = 0; i < numberOfMappers; i++) {
            mappers.add(new Mapper("mapper" + Integer.toString(i),
                    tweets.subList(i * chunkSize, (i + 1) * chunkSize)));
        }
    }

    public void runMapReduce(PartitionStrategy partitionStrategy) throws Exception {
        var futures = new ArrayList<Future<Map<String, Integer>>>();
        for (var mapper : mappers) {
            futures.add(mapperPool.submit(mapper));
        }
        mapperPool.shutdown();

        for (var future : futures) {
            var mappedData = future.get();
            switch (partitionStrategy) {
                case NAIVE:
                    partitionNaiveHash(mappedData);
                    break;
                case EQUALLY_WEIGHTED:
                    partitionSmart(mappedData);
                    break;
                default:
                    log.error("Unknown partition strategy!");
            }
        }
        reducers.forEach(reducer -> {
            var thread = new Thread(reducer);
            thread.start();
        });

    }

    public void initializeReducers(int numberOfReducers) {
        reducers = new ArrayList<>();
        for (int i = 0; i < numberOfReducers; i++) {
            var reducer = new Reducer("reducer" + Integer.toString(i));
            reducers.add(reducer);
        }
    }

    public void partitionBaseline(Map<String, Integer> mappedData) throws InterruptedException {
        var keys = mappedData.keySet().toArray();
        var counter = 0;
        var hashtagsPerReducer = mappedData.size() / reducers.size();
        for (int i = 0; i < reducers.size(); i++) {
            for (var j = 0; j < hashtagsPerReducer; j++) {
                reducers.get(i).getQueue().put(Map.of((String) keys[counter], mappedData.get((String) keys[counter])));
                counter++;
            }
        }
    }

    // Refinement One
    public void partitionSmart(Map<String, Integer> mappedData) throws InterruptedException {
        var keys = mappedData.keySet().toArray();
        var hotKeys = getHotKeys(mappedData, 500);
        var counter = 0;
        var hashtagsPerReducer = mappedData.size() / reducers.size();
        for (int i = 0; i < reducers.size(); i++) {
            for (var j = 0; j < hashtagsPerReducer; j++) {
                reducers.get(i).getQueue().put(Map.of((String) keys[counter], mappedData.get((String) keys[counter])));
                counter++;
            }
        }
    }

    private List<String> getHotKeys(Map<String, Integer> mappedData, int threshold) {
        var hotKeys = new ArrayList<String>();
        for (var entry : mappedData.entrySet()) {
            if (entry.getValue() >= threshold)
                hotKeys.add(entry.getKey());
        }
        return hotKeys;
    }

    public String collectResults() {
        var builder = new StringBuilder();
        try {
            reducers.forEach(reducer -> {
                if (reducer.getQueue().isEmpty()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                builder.append(
                        reducer.getName() + " spent " + reducer.getTimeSpent() + "ms reducing records: "
                                + reducer.getResult().toString() + "\n");
            });
        } finally {
            if (mapperPool != null && !mapperPool.isShutdown())
                mapperPool.shutdownNow();
        }
        return builder.toString();
    }
}
