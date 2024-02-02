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

    private static final int MAP_SIZE = 2 << 10; // ceiling(log(10k) / log(2))

    private static class Stations {

        private int stationPointer = 0;
        private final int[] stationOriginalIdxs = new int[MAP_SIZE];
        private final String[] stationNames = new String[MAP_SIZE];
        /*
         * i + 0, hash
         * i + 1, min
         * i + 2, max
         * i + 3, sum
         * i + 4, count
         */

        private final int MEASUREMENT_SIZE = 5;
        private final int[] measurements = new int[MAP_SIZE * MEASUREMENT_SIZE];

        boolean entryExists(int idxOffset) {
            // an entry exists if there is a hash in the measurements.
            return measurements[idxOffset] != 0;
        }

        boolean stationMatches(int idxOffset, int hash) {
            return measurements[idxOffset] == hash;
        }

        void linearProbe(int idx, int hash, int value, byte[] entryBytes, int stationLength) {
            int newIdx = idx;
            while (true) {
                if (stationMatches(newIdx, hash)) {
                    // Collision, but existing station.
                    updateStationFast(newIdx, value);
                    break;
                }
                else if (!entryExists(newIdx)) {
                    // Empty entry location, insert new statoin.
                    String stationName = new String(entryBytes, 0, stationLength, StandardCharsets.UTF_8);
                    insertStationFast(newIdx, hash, stationName, value);
                    break;
                }
                else {
                    // Not empty entry, continue to probe.
                    newIdx += MEASUREMENT_SIZE;
                }
            }
        }

        void insertOrUpdateStation(int idx, int hash, int value, byte[] entryBytes, int stationLength) {
            int idxOffset = idx * MEASUREMENT_SIZE;
            if (stationMatches(idxOffset, hash)) {
                // Check if station matches, 80% of the time it should, only 30% collision.
                updateStationFast(idxOffset, value);
            }
            else if (entryExists(idxOffset)) {
                // If entry is not empty and station doesn't match, this means there is a hash collision.
                linearProbe(idxOffset + MEASUREMENT_SIZE, hash, value, entryBytes, stationLength);
            }
            else {
                // No collision.
                String stationName = new String(entryBytes, 0, stationLength, StandardCharsets.UTF_8);
                insertStationFast(idxOffset, hash, stationName, value);
            }
        }

        void updateStationFast(int idxOffset, int value) {
            measurements[idxOffset + 1] = Math.min(measurements[idxOffset + 1], value);
            measurements[idxOffset + 2] = Math.max(measurements[idxOffset + 2], value);
            measurements[idxOffset + 3] += value;
            ++measurements[idxOffset + 4];
        }

        void insertStation(int idxOffset, int hash, String station, int min, int max, int sum, int count) {
            measurements[idxOffset + 1] = min;
            measurements[idxOffset + 2] = max;
            measurements[idxOffset + 3] = sum;
            measurements[idxOffset + 4] = count;
            measurements[idxOffset] = hash;

            stationNames[stationPointer] = station;
            stationOriginalIdxs[stationPointer] = idxOffset / MEASUREMENT_SIZE;
            ++stationPointer;
        }

        void insertStationFast(int idxOffset, int hash, String station, int value) {
            // If the station does not exist we can pass in less arguments.
            measurements[idxOffset + 1] = value;
            measurements[idxOffset + 2] = value;
            measurements[idxOffset + 3] = value;
            measurements[idxOffset + 4] = 1;
            measurements[idxOffset] = hash;

            stationNames[stationPointer] = station;
            stationOriginalIdxs[stationPointer] = idxOffset / MEASUREMENT_SIZE;
            ++stationPointer;
        }

        void mergeStation(int hash, String station, int min, int max, int sum, int count) {
            int newIdx = hash & (MAP_SIZE - 1);
            int idxOffset;
            while (true) {
                idxOffset = newIdx * MEASUREMENT_SIZE;
                if (stationMatches(idxOffset, hash)) {
                    measurements[idxOffset + 1] = Math.min(measurements[idxOffset + 1], min);
                    measurements[idxOffset + 2] = Math.max(measurements[idxOffset + 2], max);
                    measurements[idxOffset + 3] += sum;
                    measurements[idxOffset + 4] += count;
                    break;
                }
                else if (entryExists(idxOffset)) {
                    // Continue to linear probe.
                    ++newIdx;
                }
                else {
                    // If a station does not exist in a thread's map, then insert.
                    // This does not happen in our case, but it's more realistic :D.
                    insertStation(idxOffset, hash, station, min, max, sum, count);
                    break;
                }
            }
        }

        void printStation(int stationPtr) {
            int idxOffset = stationOriginalIdxs[stationPtr] * MEASUREMENT_SIZE;
            int min = measurements[idxOffset + 1];
            int max = measurements[idxOffset + 2];
            int sum = measurements[idxOffset + 3];
            int count = measurements[idxOffset + 4];
            System.out.print(
                    stationNames[stationPtr] + "="
                            + (min / 10.0) + "/"
                            + (Math.round((double) sum / (double) count) / 10.0) + "/"
                            + (max / 10.0));
        }
    }

    static class ProcessFileMap implements Callable<Stations> {
        private long readStart;
        private long readEnd;
        private boolean lastRead;
        private boolean firstRead;
        byte[] entryBytes = new byte[512];

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
            int idx = stationHash & (MAP_SIZE - 1);
            stations.insertOrUpdateStation(idx, stationHash, value, entryBytes, keyLength);
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

                Stations station1 = results.getFirst();
                for (int i = 1; i < results.size(); ++i) {
                    Stations currStation = results.get(i);
                    for (int j = 0; j < currStation.stationPointer; ++j) {
                        int currStationIdx = currStation.stationOriginalIdxs[j];
                        String station = currStation.stationNames[j];
                        int idxOffset = currStationIdx * currStation.MEASUREMENT_SIZE;
                        int min = currStation.measurements[idxOffset + 1];
                        int max = currStation.measurements[idxOffset + 2];
                        int sum = currStation.measurements[idxOffset + 3];
                        int count = currStation.measurements[idxOffset + 4];
                        int hash = currStation.measurements[idxOffset];
                        station1.mergeStation(hash, station, min, max, sum, count);
                    }
                }

                // print key and values'
                System.out.print("{");
                for (int i = 0; i < station1.stationPointer - 1; ++i) {
                    station1.printStation(i);
                    System.out.print(", ");
                }
                station1.printStation(station1.stationPointer - 1);
                System.out.print("}");
            }
        }
    }
}