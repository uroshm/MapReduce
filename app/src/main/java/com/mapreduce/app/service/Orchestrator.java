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
    List<Mapper> mappers;
    List<Reducer> reducers;
    List<BlockingQueue<Map<String, Integer>>> reducerQueues = new ArrayList<>();
    ExecutorService mapperPool;

    public void initializeMappers(List<Tweet> tweets, int numberOfMappers) {
        var chunkSize = tweets.size() / numberOfMappers;
        mapperPool = Executors.newFixedThreadPool(numberOfMappers);
        mappers = new ArrayList<>();
        for (int i = 0; i < numberOfMappers; i++) {
            mappers.add(new Mapper("mapper" + Integer.toString(i),
                    tweets.subList(i * chunkSize, (i + 1) * chunkSize)));
        }
    }

    public void runMappers(PartitionStrategy partitionStrategy) throws Exception {
        var futures = new ArrayList<Future<Map<String, Integer>>>();
        for (var mapper : mappers) {
            futures.add(mapperPool.submit(mapper));
        }
        mapperPool.shutdown();

        for (var future : futures) {
            var mappedData = future.get();
            switch (partitionStrategy) {
                case NAIVE_HASH:
                    partitionNaiveHash(mappedData);
                    break;
                case EQUALLY_WEIGHTED:
                    partitionEquallyWeighted(mappedData);
                    break;
                default:
                    log.error("Unknown partition strategy!");
            }
        }
    }

    public void initializeReducers(int numberOfReducers) {
        reducers = new ArrayList<>();
        for (int i = 0; i < numberOfReducers; i++) {
            var reducer = new Reducer("reducer" + Integer.toString(i));
            reducers.add(reducer);
        }
    }

    public List<Reducer> getAvailableReducers() {
        List<Reducer> availableReducers = new ArrayList<>();
        for (var reducer : reducers) {
            if (!reducer.isRunning()) {
                availableReducers.add(reducer);
            }
        }
        return availableReducers;
    }

    // Baseline design
    public void partitionNaiveHash(Map<String, Integer> mappedData) throws InterruptedException {
        var availableReducers = getAvailableReducers();
        if (availableReducers == null || availableReducers.isEmpty()) {
            System.err.println("Something went wrong, no reducer is available!");
            return;
        }

        for (var entry : mappedData.entrySet()) {
            var hashtag = entry.getKey();
            var reducerIndex = Math.abs(hashtag.hashCode()) % availableReducers.size();
            availableReducers.get(reducerIndex).getQueue()
                    .put(Map.of(entry.getKey(), entry.getValue()));
        }
    }

    // Refinement One: Equally distribute workload based on number of keys
    public void partitionEquallyWeighted(Map<String, Integer> mappedData) throws InterruptedException {
        var availableReducers = getAvailableReducers();
        if (availableReducers == null || availableReducers.isEmpty()) {
            System.err.println("Something went wrong, no reducer is available!");
            return;
        }
        var workloadPerReducer = mappedData.size() / availableReducers.size();
        var entries = new ArrayList<>(mappedData.entrySet());
        int entryIndex = 0;

        for (var reducer : availableReducers) {
            var itemsAdded = 0;
            while (itemsAdded < workloadPerReducer && entryIndex < entries.size()) {
                var entry = entries.get(entryIndex);
                reducer.getQueue().put(Map.of(entry.getKey(), entry.getValue()));
                itemsAdded++;
                entryIndex++;
            }
        }

        while (entryIndex < entries.size()) {
            var entry = entries.get(entryIndex);
            availableReducers.get(availableReducers.size() - 1).getQueue()
                    .put(Map.of(entry.getKey(), entry.getValue()));
            entryIndex++;
        }
    }
}
