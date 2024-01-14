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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public class CalculateAverage_franklyn {
    private static final String FILE = "./measurements.txt";

    private static class MappedFileReader {
        private MappedByteBuffer buffer;
        private FileChannel fch;
        private int readSize;
        private long fileSize;
        private long bytesRead;

        public MappedFileReader(FileChannel fch, int readSize) throws IOException {
            this.fch = fch;
            this.readSize = readSize;
            this.fileSize = fch.size();
            this.bytesRead = 0;
            this.buffer = fch.map(FileChannel.MapMode.READ_ONLY, 0, readSize);
        }

        public int get(byte[] readBytes) throws IOException {
            if (fileSize - bytesRead <= 0) {
                return -1;
            }

            int remainingBytes = buffer.limit() - buffer.position();
            int bytesToRead = Math.min(readBytes.length, remainingBytes);
            int validBytes = bytesToRead;
            buffer.get(readBytes, 0, bytesToRead);
            bytesRead += bytesToRead;

            // If we are done with this chunk of memory, map the next section in.
            if (readBytes.length > (buffer.limit() - buffer.position())) {
                long remainingFileBytes = fileSize - bytesRead;
                int partialRead = (int) Math.min(readBytes.length - bytesToRead, remainingFileBytes);
                buffer = fch.map(FileChannel.MapMode.READ_ONLY, bytesRead, Math.min(readSize, remainingFileBytes));
                buffer.get(readBytes, bytesToRead, partialRead);
                bytesRead += partialRead;
                validBytes += partialRead;
            }

            return validBytes;
        }
    }

    public static void main(String[] args) throws IOException {
        //Path measurements = Paths.get("/Users/franklyndsouza/src/github.com/damnMeddlingKid/1brc/src/test/resources/samples/measurements-3.txt");
        Path measurements = Paths.get(".", FILE);

        final int segmentSize = 4096;

        // The max mem we can map is 2GB
        final int readSize = segmentSize; //Integer.MAX_VALUE;

        final int numSegmentsToRead = (readSize / segmentSize) - 1; //we read one less segment than what we map so that we can overlap with the next read.

        HashMap<String, double[]> aggMap = new HashMap<>();
        byte[] segment = new byte[segmentSize];

        try(FileChannel fileChannel = (FileChannel) Files.newByteChannel(measurements, EnumSet.of(StandardOpenOption.READ))) {
            MappedFileReader reader = new MappedFileReader(fileChannel, (int) Math.min(readSize, fileChannel.size()));

            int start = 0;
            int end = 0;

            reader.get(segment);
            int bytesRead = segment.length;
            byte[] keyBytes;
            byte[] valueBytes;

            while (true) {
                while(end < bytesRead && (segment[end] ^ ';') != 0) end++;

                int length = end - start;
                keyBytes = new byte[end - start];
                System.arraycopy(segment, start, keyBytes, 0, length);

                // We've read passed the end of the segment without finding the key
                if(end == bytesRead) {
                    bytesRead = reader.get(segment);
                    if(bytesRead == -1) break;
                    end = 0;
                    while(end < bytesRead && (segment[end] ^ ';') != 0) end++; // we can technically remove the length check here since we just started the segment.
                    byte[] temp = keyBytes;
                    keyBytes = new byte[temp.length + end];

                    System.arraycopy(temp, 0, keyBytes, 0, temp.length); // copy the partial key we found.
                    System.arraycopy(segment, 0, keyBytes, temp.length, end); // copy the rest of the key.
                }

                start = end + 1;
                while(end < bytesRead && (segment[end] ^ '\n') != 0) end++;

                length = end - start;
                valueBytes = new byte[length];
                System.arraycopy(segment, start, valueBytes, 0, length);

                // We've read passed the end of the segment without finding the value
                if(end == bytesRead) {
                    bytesRead = reader.get(segment);
                    if(bytesRead == -1) {
                        aggregateValue(aggMap, keyBytes, valueBytes);
                        break;
                    }
                    end = 0;
                    while(end < bytesRead && (segment[end] ^ '\n') != 0) end++;
                    byte[] temp = valueBytes;
                    valueBytes = new byte[temp.length + end];
                    System.arraycopy(temp, 0, valueBytes, 0, temp.length); // copy the partial key we found.
                    System.arraycopy(segment, 0, valueBytes, temp.length, end); // copy the rest of the key.
                }

                start = end + 1;
                aggregateValue(aggMap, keyBytes, valueBytes);
            }
        }

        // print key and values in map sorted alphabetically
        for (Map.Entry<String, double[]> entry : aggMap.entrySet()) {
            double[] value = entry.getValue();
            double min = value[0];
            double max = value[1];
            double sum = value[2];
            double count = value[3];
            //System.out.println(entry.getKey()+"="+ String.format("%.1f", min) + "/" + String.format("%.1f", sum/count) + "/" + String.format("count %.1f", count) + "/" + String.format("%.1f", max));
            System.out.println(STR."\{entry.getKey()}=\{String.format("%.1f", min)}/\{String.format("%.1f", sum / count)}/\{String.format("%.1f", max)}");
        }

        //System.out.println(aggMap);
    }

    private static void aggregateValue(HashMap<String, double[]> aggMap, byte[] keyBytes, byte[] valueBytes) {
        // TODO remove this string allocation using an int map
        aggMap.compute(new String(keyBytes), (_, v) -> {
            double value = Double.parseDouble(new String(valueBytes));
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
}
