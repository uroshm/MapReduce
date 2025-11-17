package com.mapreduce.app.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.springframework.stereotype.Service;

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
            mappers.add(new Mapper("mapper" + Integer.toString(i), tweets.subList(i * chunkSize, (i + 1) * chunkSize)));
        }
    }

    public void runMappers() throws Exception {
        var futures = new ArrayList<Future<Map<String, Integer>>>();
        for (var mapper : mappers) {
            futures.add(mapperPool.submit(mapper));
        }
        mapperPool.shutdown();

        for (var future : futures) {
            var mappedData = future.get();
            partition(mappedData);
        }
    }

    public void initializeReducers(int numberOfReducers) {
        reducers = new ArrayList<>();
        for (int i = 0; i < numberOfReducers; i++) {
            var reducer = new Reducer("reducer" + Integer.toString(i), new LinkedBlockingQueue<>());
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

    public void partition(Map<String, Integer> mappedData) throws InterruptedException {
        var availableReducers = getAvailableReducers();
        if (availableReducers == null || availableReducers.isEmpty()) {
            System.err.println("Something went wrong, no reducer is available!");
            return;
        }

        var workloadPerReducer = mappedData.size() / availableReducers.size();
        var reducerCounter = 0;
        for(var i = 0; i < mappedData.size(); i++) {
            var availableReducer = availableReducers.get(i);
            availableReducer.reserve();             
            // TODO: implement logic here
        }
    }
}
