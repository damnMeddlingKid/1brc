/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class CalculateAverage_ericxiao {

    private static final String FILE = "./measurements_125M.txt";

    private static class Station {
        private double min;
        private double max;
        private double mean;
        private double sum;
        private int count;

        private Station(Double temp) {
            this.min = temp;
            this.max = temp;
            this.sum = temp;
            this.count = 1;
        }

        public void setMeasurement(double value) {
            this.min = Math.min(this.min, value);
            this.max = Math.max(this.max, value);
            this.sum += value;
            this.count++;
        }

        public void setMean() {
            this.mean = this.sum / this.count;
        }

        public void mergeStation(Station station) {
            this.min = Math.min(this.min, station.min);
            this.max = Math.max(this.max, station.max);
            this.sum += station.sum;
            this.count += station.count;
        }

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

    }

    static class MyCallable implements Callable<Map<String, Station>> {

        String fileName;
        private Map<String, Station> measurements;

        public MyCallable(String fileName) {
            this.fileName = fileName;
            this.measurements = new HashMap<>();
        }

        private void processLine(String line) {
            String[] rawLine = line.split(";");
            double measurement = Double.parseDouble(rawLine[1]);
            if (measurements.containsKey(rawLine[0])) {
                measurements.get(rawLine[0]).setMeasurement(measurement);
            }
            else {
                measurements.put(rawLine[0], new Station(measurement));
            }
        }

        @Override
        public Map<String, Station> call() throws Exception {
            // Code to be executed in the new thread
            try {
                BufferedReader reader = new BufferedReader(new FileReader(this.fileName));
                reader.lines().forEach(this::processLine);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            return measurements;
        }
    }

    public static void main(String[] args) throws Exception {
        // Something wrong with main thread, use 7 for now.
        int numThreads = Runtime.getRuntime().availableProcessors() - 1; // Use the number of available processors
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        List<Callable<Map<String, Station>>> callableTasks = new ArrayList<>();
        for (int i = 0; i < numThreads; ++i) {
            MyCallable callableTask = new MyCallable(FILE);
            callableTasks.add(callableTask);
        }

        List<Map<String, Station>> results = new ArrayList<>();
        try {
            List<Future<Map<String, Station>>> futures = executorService.invokeAll(callableTasks);
            for (Future<Map<String, Station>> future : futures) {
                try {
                    results.add(future.get());

                }
                catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            executorService.shutdown();
            Map<String, Station> mapA = results.get(0);
            for (int i = 1; i < numThreads; ++i) {
                results.get(i).forEach((station, stationMeasurements) -> {
                    if (mapA.containsKey(station)) {
                        mapA.get(station).mergeStation(stationMeasurements);
                    }
                    else {
                        mapA.put(station, stationMeasurements);
                    }
                });
            }
            System.out.println(mapA);
        }
    }
}