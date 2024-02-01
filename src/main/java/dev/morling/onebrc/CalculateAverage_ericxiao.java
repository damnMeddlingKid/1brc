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
        private final int[] stationHashes = new int[MAP_SIZE];
        private final String[] stationNames = new String[MAP_SIZE];
        /*
         * i + 0, min
         * i + 1, max
         * i + 2, sum
         * i + 3, count
         */

        private final int MEASUREMENT_SIZE = 4;
        private final int[] measurements = new int[MAP_SIZE * MEASUREMENT_SIZE];

        boolean stationExists(int hash) {
            // if count != 0, that means we have never seen this before.
            int idx = hash * MEASUREMENT_SIZE;
            return measurements[idx] != 0;
        }

        void insertStation(int hash, String station, int min, int max, int sum, int count) {
            int idx = hash * MEASUREMENT_SIZE;
            measurements[idx] = count;
            measurements[idx + 1] = min;
            measurements[idx + 2] = max;
            measurements[idx + 3] = sum;

            stationNames[stationPointer] = station;
            stationHashes[stationPointer] = hash;
            stationPointer++;
        }

        void insertOrUpdate(int hash, int value, byte[] entryBytes, int keyLength) {
            int idx = hash * MEASUREMENT_SIZE;
            if (measurements[idx] == 0) {
                measurements[idx] = 1;
                measurements[idx + 1] = value;
                measurements[idx + 2] = value;
                measurements[idx + 3] = value;
                stationNames[stationPointer] = new String(entryBytes, 0, keyLength, StandardCharsets.UTF_8);;
                stationHashes[stationPointer] = hash;
                stationPointer++;
            } else {
                measurements[idx] += 1;
                measurements[idx + 1] = Math.min(measurements[idx], value);
                measurements[idx + 2] = Math.max(measurements[idx + 1], value);
                measurements[idx + 3] += value;
            }
        }

        void updateStation(int hash, int min, int max, int sum, int count) {
            int idx = hash * MEASUREMENT_SIZE;
            measurements[idx] += count;
            measurements[idx + 1] = Math.min(measurements[idx], min);
            measurements[idx + 2] = Math.max(measurements[idx + 1], max);
            measurements[idx + 3] += sum;
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

        public int calcValue(int valueStart, int valueLength) {
            final byte negativeSign = '-';
            int accumulator = 0;

            if (entryBytes[valueStart] == negativeSign) {
                for (int i = valueStart + 1; i < valueStart + valueLength - 2; ++i) {
                    accumulator = accumulator * 10 + entryBytes[i] - '0';
                }
                return -accumulator*10;
            }
            else {
                accumulator = entryBytes[valueStart] - '0';
                for (int i = valueStart + 1; i < valueStart + valueLength - 2; ++i) {
                    accumulator = accumulator * 10 + entryBytes[i] - '0';
                }
                return accumulator*10;
            }
        }

        public void add(int stationHash, long keyStart, long keyEnd, long valueEnd) {
            int keyLength = (int) (keyEnd - keyStart);
            // Calculate measurement
            int valueLength = (int) (valueEnd - (keyEnd + 1));

            int value = calcValue(keyLength + 1, valueLength);
            // Calculate station
            int hash = stationHash & (MAP_SIZE - 1);

            stations.insertOrUpdate(hash, value, entryBytes, keyLength);
        }

        public Stations call() {
            return readMemory(readStart, readEnd);
        }

        private Stations readMemory(long startAddress, long endAddress) {
            long keyEndAddress;
            long valueEndAddress;
            long byteStart = startAddress;
            long keyStartAddress = byteStart;

            if (!firstRead) {
                while (UNSAFE.getByte(byteStart++) != '\n')
                    ;
                keyStartAddress = byteStart;
                byteStart--;
            }

            int byteIndex;
            long stationHash;

            //TODO: bounds are wrong here
            while (byteStart < endAddress - 1) {
                byteIndex = -1;
                stationHash = 0;
                while ((entryBytes[++byteIndex] = UNSAFE.getByte(++byteStart)) != ';') {
                    stationHash = 31 * stationHash + (entryBytes[byteIndex] & 0xff);
                }

                keyEndAddress = byteStart;

                while ((entryBytes[byteIndex++] = UNSAFE.getByte(++byteStart)) != '\n')
                    ;

                valueEndAddress = byteStart;
                add((int) stationHash, keyStartAddress, keyEndAddress, valueEndAddress);
                keyStartAddress = valueEndAddress + 1;
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
                for (int i = 1; i < numThreads; ++i) {
                    Stations currStation = results.get(i);
                    for (int j = 0; j < currStation.stationPointer; j++) {
                        int currStationHash = currStation.stationHashes[j];
                        int idx = currStationHash * currStation.MEASUREMENT_SIZE;
                        int count = currStation.measurements[idx];
                        int min = currStation.measurements[idx + 1];
                        int max = currStation.measurements[idx + 2];
                        int sum = currStation.measurements[idx + 3];
                        if (station1.stationExists(currStationHash))
                            station1.updateStation(currStationHash, min, max, sum, count);
                        else
                            station1.insertStation(currStationHash, currStation.stationNames[j], min, max, sum, count);
                    }
                }
                // print key and values
                System.out.print("{");
                for (int i = 0; i < station1.stationPointer - 1; i++) {
                    int idx = station1.stationHashes[i] * station1.MEASUREMENT_SIZE;
                    int count = station1.measurements[idx];
                    int min = station1.measurements[idx + 1];
                    int max = station1.measurements[idx + 2];
                    int sum = station1.measurements[idx + 3];
                    System.out.print(
                            station1.stationNames[i] + "="
                                    + (min / 10.0) + "/"
                                    + (Math.round((double) sum / (double) count) / 10.0) + "/"
                                    + (max / 10.0)
                                    + ", ");
                }

                int idx = station1.stationHashes[station1.stationPointer - 1] * station1.MEASUREMENT_SIZE;
                int count = station1.measurements[idx];
                int min = station1.measurements[idx + 1];
                int max = station1.measurements[idx + 2];
                int sum = station1.measurements[idx + 3];
                System.out.print(
                        station1.stationNames[station1.stationPointer - 1] + "="
                                + (min / 10.0) + "/"
                                + (Math.round((double) sum / (double) count) / 10.0) + "/"
                                + (max / 10.0));

                System.out.print("}");
            }
        }
    }
}