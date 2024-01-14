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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;

public class CalculateAverage_ericxiao {

    private static final String FILE = "./measurements.txt";

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

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

    }

    private static Map<String, Station> measurements = new TreeMap<>();

    private static void processLine(String line) {
        String[] rawLine = line.split(";");
        String station = rawLine[0];
        double measurement = Double.parseDouble(rawLine[1]);
        if (measurements.containsKey(station)) {
            measurements.get(station).setMeasurement(measurement);
        }
        else {
            measurements.put(station, new Station(measurement));
        }
    }

    public static void main(String[] args) throws IOException {
        Files.lines(Paths.get(FILE)).forEach(CalculateAverage_ericxiao::processLine);

        measurements.values().forEach(Station::setMean);

        System.out.println(measurements);
    }
}