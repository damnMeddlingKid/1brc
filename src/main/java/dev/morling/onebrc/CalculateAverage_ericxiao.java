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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;

public class CalculateAverage_ericxiao {

    private static final String FILE = "./measurements.txt";

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

    static class ProcessFileMap implements Callable<Map<String, double[]>> {
        private Path filePath;
        private long readStart;
        private int readLength;
        private int segmentSize;
        private boolean lastRead;

        public ProcessFileMap(Path filePath, long readStart, int readLength, int segmentSize, boolean lastRead) {
            this.filePath = filePath;
            this.readStart = readStart;
            this.readLength = readLength;
            this.segmentSize = segmentSize;
            this.lastRead = lastRead;
        }

        private static class MappedFileReader {
            private MappedByteBuffer buffer;
            private FileChannel fch;

            public MappedFileReader(Path filePath, long offset, int length) throws IOException {
                try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(filePath, EnumSet.of(StandardOpenOption.READ))) {
                    this.fch = fileChannel;
                    this.buffer = fch.map(FileChannel.MapMode.READ_ONLY, offset, length);
                }
            }

            public int get(byte[] readBytes) {
                if (buffer.limit() - buffer.position() == 0) {
                    return -1;
                }

                int remainingBytes = buffer.limit() - buffer.position();
                int validBytes = Math.min(readBytes.length, remainingBytes);
                buffer.get(readBytes, 0, validBytes);
                return validBytes;
            }
        }

        private static class ReadPosition {
            public int position;
            public int bytesRead;
            public int validBytes;

            public ReadPosition() {
                this.position = 0;
                this.bytesRead = 0;
                this.validBytes = 4096;
            }
        }

        private int seekTill(byte[] segment, int validBytes, int start, char delimiter) {
            // returns either the index of the delimiter or one after the end of the segment
            while (start < validBytes && (segment[start] ^ delimiter) != 0)
                start++;
            return start;
        }

        private void readTill(MappedFileReader reader, byte[] src, ReadPosition srcHead, byte[] desintation, int destOffset, char delimiter) {
            if (srcHead.position == -1) {
                return;
            }

            if (srcHead.position == src.length) {
                srcHead.position = 0;
                srcHead.validBytes = reader.get(src);
            }

            int end = seekTill(src, srcHead.validBytes, srcHead.position, delimiter);
            int length = end - srcHead.position;
            System.arraycopy(src, srcHead.position, desintation, destOffset, length);
            srcHead.bytesRead = length;

            // We've read more than a segment without finding the delimiter
            if (end == srcHead.validBytes) {
                srcHead.position = 0;
                srcHead.validBytes = reader.get(src);

                // We have read all bytes in the mapping.
                if (srcHead.validBytes == -1) {
                    srcHead.position = -1;
                    return;
                }

                end = seekTill(src, srcHead.validBytes, 0, delimiter);
                System.arraycopy(src, srcHead.position, desintation, destOffset + length, end);
                srcHead.bytesRead += end;
            }

            srcHead.position = end;
        }

        private static void aggValue(HashMap<String, double[]> aggMap, String key, double value) {
            aggMap.compute(key, (_, v) -> {
                if (v == null) {
                    return new double[]{ value, value, value, 1 };
                }
                else {
                    v[0] = Math.min(v[0], value);
                    v[1] = Math.max(v[1], value);
                    v[2] = v[2] + value;
                    v[3] = v[3] + 1;
                    return v;
                }
            });
        }

        @Override
        public Map<String, double[]> call() throws IOException {
            String key = "";
            double value;

            HashMap<String, double[]> aggMap = new HashMap<>();
            MappedFileReader reader = new MappedFileReader(filePath, this.readStart, this.readLength);
            ReadPosition parseHead = new ReadPosition();

            byte[] segment = new byte[this.segmentSize];
            byte[] keyBytes = new byte[100];
            byte[] valueBytes = new byte[100];
            boolean partialValue = false;

            parseHead.validBytes = reader.get(segment);

            if (this.readStart != 0) {
                parseHead.position = seekTill(segment, parseHead.validBytes, 0, '\n');
                parseHead.position++;
            }

            while (true) {
                readTill(reader, segment, parseHead, keyBytes, 0, ';');
                if (parseHead.position == -1) {
                    break;
                }
                key = new String(keyBytes, 0, parseHead.bytesRead, StandardCharsets.UTF_8);
                parseHead.position++;
                parseHead.bytesRead = 0;
                readTill(reader, segment, parseHead, valueBytes, 0, '\n');
                if (parseHead.position == -1 && !lastRead) {
                    partialValue = true;
                    break;
                }
                value = Double.parseDouble(new String(valueBytes, 0, parseHead.bytesRead, StandardCharsets.UTF_8));
                parseHead.position++;
                parseHead.bytesRead = 0;
                aggValue(aggMap, key, value);
            }

            if (!lastRead) {
                reader = new MappedFileReader(filePath, readStart + readLength, 4096);
                reader.get(segment);
                ReadPosition head = new ReadPosition();
                if (!partialValue) {
                    readTill(reader, segment, head, keyBytes, parseHead.bytesRead, ';');
                    key = new String(keyBytes, 0, parseHead.bytesRead + head.bytesRead, StandardCharsets.UTF_8);
                }
                if (partialValue) {
                    readTill(reader, segment, head, valueBytes, 0, '\n');
                    value = Double.parseDouble(new String(valueBytes, 0, parseHead.bytesRead + head.bytesRead, StandardCharsets.UTF_8));
                }
                else {
                    head.position++;
                    readTill(reader, segment, head, valueBytes, 0, '\n');
                    value = Double.parseDouble(new String(valueBytes, 0, head.bytesRead, StandardCharsets.UTF_8));
                }
                aggValue(aggMap, key, value);
            }

            return aggMap;
        }
    }

    public static void main(String[] args) throws Exception {
        int numThreads = Runtime.getRuntime().availableProcessors(); // Use the number of available processors
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        List<Callable<Map<String, double[]>>> callableTasks = new ArrayList<>();
        Path filePath = Path.of(FILE);
        long fileSize = Files.size(filePath);
        int segmentSize = 4096;
        int readLength = (int) (fileSize / numThreads);
        long readStart = 0;

        for (int i = 0; i < numThreads - 1; ++i) {
            ProcessFileMap callableTask = new ProcessFileMap(filePath, readStart, readLength, segmentSize, false);
            readStart += readLength;
            callableTasks.add(callableTask);
        }

        callableTasks.add(new ProcessFileMap(filePath, readStart, (int) (fileSize - readStart), segmentSize, true));

        List<Map<String, double[]>> results = new ArrayList<>();
        try {
            List<Future<Map<String, double[]>>> futures = executorService.invokeAll(callableTasks);
            for (Future<Map<String, double[]>> future : futures) {
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
            Map<String, double[]> mapA = results.getFirst();
            for (int i = 1; i < numThreads; ++i) {
                results.get(i).forEach((station, stationMeasurements) -> {
                    if (mapA.containsKey(station)) {
                        double[] measurements = mapA.get(station);
                        measurements[0] = Math.min(measurements[0], stationMeasurements[0]);
                        measurements[1] = Math.max(measurements[1], stationMeasurements[1]);
                        measurements[2] = measurements[2] + stationMeasurements[2];
                        measurements[3] = measurements[3] + stationMeasurements[3];
                    }
                    else {
                        mapA.put(station, stationMeasurements);
                    }
                });
            }
            // print key and values

            for (Map.Entry<String, double[]> entry : mapA.entrySet()) {
                double[] measurements = entry.getValue();
                System.out.println("-" + entry.getKey() + ": " + measurements[0] + "/" + measurements[1] + "/" + measurements[2] / measurements[3]);
            }

            // System.out.println(mapA);
        }
    }
}