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

import sun.misc.Unsafe;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;

public class CalculateAverage_ericxiao {

    private static final String FILE = "./measurements.txt";

    private static final int MAP_SIZE = 2 << 11; // ceiling(log(10k) / log(2))

    private static class Station {
        public int min;
        public int max;
        public int sum;
        public int count;
        public String name;

        public void set(int value, String name) {
            this.min = value;
            this.max = value;
            this.sum = value;
            this.count = 1;
            this.name = name;
        }

        public void merge(int value) {
            this.min = Math.min(this.min, value);
            this.max = Math.max(this.max, value);
            this.sum += value;
            this.count++;
        }

        public Station merge(Station other) {
            if (this.name != null && !this.name.equals(other.name))
                throw new RuntimeException("wrong");
            this.min = Math.min(this.min, other.min);
            this.max = Math.max(this.max, other.max);
            this.sum += other.sum;
            this.count += other.count;
            return this;
        }

        public String toString() {
            return STR."\{this.min / 10.0}/\{Math.round((double) this.sum / (double) this.count) / 10.0}/\{this.max / 10.0}";
        }
    }

    private static class Stations {
        private int tail = 0;
        private final Station[] measurements;
        public final int[] stationPositions = new int[512];

        public Stations() {
            measurements = new Station[MAP_SIZE];
            for (int i = 0; i < MAP_SIZE; i++) {
                measurements[i] = new Station();
            }
        }

        void insertOrUpdate(int hash, int value, byte[] entryBytes, int keyLength) {
            if (measurements[hash].count == 0) {
                measurements[hash].set(value, new String(entryBytes, 0, keyLength, StandardCharsets.UTF_8));
                stationPositions[tail++] = hash;
            }
            else {
                measurements[hash].merge(value);
            }
        }
    }

    static class ProcessFileMap implements Callable<Stations> {
        private long readStart;
        private long readEnd;
        private boolean lastRead;
        private boolean firstRead;
        byte[] entryBytes = new byte[1024];

        private static final Unsafe UNSAFE = initUnsafe();

        public ProcessFileMap(long readStart, long readEnd, boolean firstRead, boolean lastRead) {
            this.readStart = readStart;
            this.readEnd = readEnd;
            this.lastRead = lastRead;
            this.firstRead = firstRead;
        }

        private final Stations stations = new Stations();

        private static Unsafe initUnsafe() {
            try {
                final Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
                theUnsafe.setAccessible(true);
                return (Unsafe) theUnsafe.get(Unsafe.class);
            }
            catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        public void add(int stationHash, int keyLength, int value) {
            // Calculate station
            int hash = stationHash & (MAP_SIZE - 1);
            stations.insertOrUpdate(hash, value, entryBytes, keyLength);
        }

        public Stations call() {
            return readMemory(readStart, readEnd);
        }

        private Stations readMemory(long startAddress, long endAddress) {
            long byteStart = startAddress;

            if (!firstRead) {
                while (UNSAFE.getByte(byteStart++) != '\n')
                    ;
                byteStart--;
            }

            int byteIndex;
            long stationHash;

            // TODO: bounds are wrong here
            while (byteStart < endAddress - 1) {
                byteIndex = -1;
                stationHash = 0;
                while ((entryBytes[++byteIndex] = UNSAFE.getByte(++byteStart)) != ';') {
                    stationHash = 31 * stationHash + (entryBytes[byteIndex] & 0xff);
                }

                byte value = UNSAFE.getByte(++byteStart);
                int accumulator = 0;
                int keyLength = byteIndex;

                if (value == '-') {
                    while ((value = UNSAFE.getByte(++byteStart)) != '.') {
                        accumulator = accumulator * 10 + value - '0';
                    }
                    add((int) stationHash, keyLength, -(accumulator * 10 + UNSAFE.getByte(++byteStart) - '0'));
                    byteStart++;
                }
                else {
                    accumulator = value - '0';
                    while ((value = UNSAFE.getByte(++byteStart)) != '.') {
                        accumulator = accumulator * 10 + value - '0';
                    }
                    add((int) stationHash, keyLength, accumulator * 10 + UNSAFE.getByte(++byteStart) - '0');
                    byteStart++;
                }
            }

            return stations;
        }
    }

    public static void main(String[] args) throws Exception {
        int numThreads = Runtime.getRuntime().availableProcessors();
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        List<Callable<Stations>> callableTasks = new ArrayList<>();
        Path filePath = Path.of(FILE);

        try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(filePath, EnumSet.of(StandardOpenOption.READ))) {
            MemorySegment fileMap = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size(), Arena.global());

            long readStart = fileMap.address();
            long fileSize = fileChannel.size();
            long readLength = (fileSize / numThreads);

            callableTasks.add(new ProcessFileMap(readStart, readStart + readLength, true, false));
            readStart += readLength;

            for (int i = 1; i < numThreads - 1; ++i) {
                ProcessFileMap callableTask = new ProcessFileMap(readStart, readStart + readLength, false, false);
                readStart += readLength;
                callableTasks.add(callableTask);
            }

            callableTasks.add(new ProcessFileMap(readStart, readStart + readLength, false, true));

            List<Stations> results = new ArrayList<>();
            try {
                List<Future<Stations>> futures = executorService.invokeAll(callableTasks);
                for (Future<Stations> future : futures) {
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
                TreeMap<String, Station> finalResults = new TreeMap<>();

                for (int i = 0; i < numThreads; ++i) {
                    Stations stations = results.get(i);
                    for (int j = 0; j < stations.tail; j++) {
                        Station station = stations.measurements[stations.stationPositions[j]];
                        finalResults.compute(station.name, (_, value) -> {
                            if (value == null)
                                return new Station().merge(station);
                            return value.merge(station);
                        });
                    }
                }

                System.out.println(finalResults);
            }
        }
    }
}