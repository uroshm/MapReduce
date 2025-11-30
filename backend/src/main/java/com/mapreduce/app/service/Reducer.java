package com.mapreduce.app.service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Data
public class Reducer implements Runnable {
    private final String name;
    private BlockingQueue<Map<String, Integer>> queue = new LinkedBlockingQueue<>();
    private boolean isRunning = false;
    private long recordsProcessed = 0;
    private long timeSpent = 0;
    private int POLL_TIME_SECONDS = 1;
    private Map<String, Integer> result = new HashMap<>();

    @Override
    public void run() {
        isRunning = true;
        long startTime = System.currentTimeMillis();

        try {
            while (true) {
                Map<String, Integer> batch = queue.poll(POLL_TIME_SECONDS, TimeUnit.SECONDS);

                if (batch == null)
                    break;
                for (var e : batch.entrySet()) {
                    Thread.sleep(e.getValue());
                    result.merge(e.getKey(), e.getValue(), Integer::sum);
                    System.out.println(this.getName() + " " + e.getValue() + " "+System.currentTimeMillis());
                    recordsProcessed++;
                }
            }

            timeSpent = System.currentTimeMillis() - startTime;
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
