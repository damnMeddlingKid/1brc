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
        private int min;
        private int max;
        private long sum;
        private int count;

        private Station(int temp) {
            this.min = temp;
            this.max = temp;
            this.sum = temp;
            this.count = 1;
        }

        public void setMeasurement(int value) {
            this.min = Math.min(this.min, value);
            this.max = Math.max(this.max, value);
            this.sum += value;
            this.count++;
        }

        public void mergeStation(Station station) {
            this.min = Math.min(this.min, station.min);
            this.max = Math.max(this.max, station.max);
            this.sum += station.sum;
            this.count += station.count;
        }

        public String toString() {
            return round(min / 10.0) + "/" + round((double) this.sum / this.count / 10.0) + "/" + round(max / 10.0);
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
            this.measurements = new HashMap<>(10000);
        }

        private void processLine(String line) {
            int separator = line.indexOf(";");

            String station = line.substring(0, separator);

            int length = line.length();
            int measurement = (Integer.parseInt(line.substring(separator + 1, length - 2)) * 10 + // end is - 2, for decimal + one fractional digit.
                    line.charAt(length - 1) - '0');

            if (measurements.containsKey(station)) {
                measurements.get(station).setMeasurement(measurement);
            }
            else {
                measurements.put(station, new Station(measurement));
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
        int numThreads = Runtime.getRuntime().availableProcessors(); // Use the number of available processors
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