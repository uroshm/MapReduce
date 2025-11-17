package com.mapreduce.app.service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Data
public class Reducer implements Runnable {
    private final String name;
    private final BlockingQueue<Map<String, Integer>> queue;
    private boolean isRunning;
    private long recordsProcessed;
    private long timeSpent;
    private int POLL_TIME_SECONDS = 2;

    @Override
    public void run() {
        isRunning = true;
        long startTime = System.currentTimeMillis();
        var result = new HashMap<String, Integer>();
        try {
            while (true) {
                Map<String, Integer> batch = queue.poll(POLL_TIME_SECONDS, TimeUnit.SECONDS);
                if (batch == null)
                    break;
                for (var e : batch.entrySet()) {       
                    result.merge(e.getKey(), e.getValue(), Integer::sum);
                }
            }

            timeSpent = System.currentTimeMillis() - startTime;
            System.out
                    .println("Reducing by " + name + " finished. Processed " + result.size() + " hashtags and took "
                            + timeSpent + "ms");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            isRunning = false;
        }
    }

    public void reserve() {
        isRunning = true;
    }

}
