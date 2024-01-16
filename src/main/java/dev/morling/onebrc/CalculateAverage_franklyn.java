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
        private long fileSize;
        private long bytesRead;

        public MappedFileReader(Path filePath, int offset, int length) throws IOException {
            try(FileChannel fileChannel = (FileChannel) Files.newByteChannel(filePath, EnumSet.of(StandardOpenOption.READ))) {
                this.fch = fileChannel;
                this.fileSize = fch.size();
                this.bytesRead = 0;
                this.buffer = fch.map(FileChannel.MapMode.READ_ONLY, offset, length);
            }
        }

        public int get(byte[] readBytes) {
            if (fileSize - bytesRead == 0) {
                return -1;
            }

            // remove this assertion in production
            if (fileSize - bytesRead < 0) {
                throw new IllegalStateException("we've read more bytes than there are in the file.");
            }

            int remainingBytes = buffer.limit() - buffer.position();
            int validBytes = Math.min(readBytes.length, remainingBytes);
            buffer.get(readBytes, 0, validBytes);
            bytesRead += validBytes;

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
        while(start < validBytes && (segment[start] ^ delimiter) != 0) start++;
        return start;
    }

    private void readTill(MappedFileReader reader, byte[] src, ReadPosition srcHead, byte[] desintation, int destOffset, char delimiter) {
        int srcStart = srcHead.position;

        if(srcStart == -1) {
            return;
        }

        int end = seekTill(src, srcHead.validBytes, srcStart, delimiter);
        int length = end - srcStart;
        System.arraycopy(src, srcStart, desintation, destOffset, length);
        srcHead.bytesRead = length;

        // We've read more than a segment without finding the delimiter
        if(end == srcHead.validBytes) {
            srcHead.position = 0;
            srcHead.validBytes = reader.get(src);

            // We have read all bytes in the mapping.
            if(srcHead.validBytes == -1) {
                srcHead.position = -1;
                return;
            }
            end = seekTill(src, srcHead.validBytes, 0, delimiter);
            System.arraycopy(src, srcStart, desintation, destOffset + length, end);
            srcHead.bytesRead += end;
        }

        srcHead.position = end;
    }

    private static void aggValue(HashMap<String, double[]> aggMap, String key, double value) {
        // TODO remove this string allocation using an int map
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

    private void doRead(Path path, int start, int length, boolean isLastRead) throws IOException
    {
        HashMap<String, double[]> aggMap = new HashMap<>();
        MappedFileReader reader = new MappedFileReader(path, start, length);

        ReadPosition parseHead = new ReadPosition();
        // TODO : get this size from the class
        byte[] segment = new byte[4096];
        // TODO : get this size from the class
        byte[] keyBytes = new byte[100];
        byte[] valueBytes = new byte[100];
        boolean partialValue = false;
        String key = "";
        double value;

        parseHead.validBytes = reader.get(segment);

        while (true) {
            readTill(reader, segment, parseHead, keyBytes, 0, ';');
            if(parseHead.position == -1) {
                break;
            }
            key = new String(keyBytes, 0, parseHead.bytesRead, StandardCharsets.UTF_8);
            readTill(reader, segment, parseHead, valueBytes, 0, '\n');
            if(parseHead.position == -1 && !isLastRead) {
                partialValue = true;
                break;
            }
            value = Double.parseDouble(new String(valueBytes, 0, parseHead.bytesRead, StandardCharsets.UTF_8));
            aggValue(aggMap, key, value);
        }

        if(!isLastRead) {
            reader = new MappedFileReader(path, start + length, 4096);
            reader.get(segment);
            ReadPosition position = new ReadPosition();
            if(!partialValue) {
                readTill(reader, segment, position, keyBytes, 0, ';');
                key = new String(keyBytes, 0, parseHead.bytesRead + position.bytesRead, StandardCharsets.UTF_8);
            }
            readTill(reader, segment, position, valueBytes, 0, '\n');
            if(partialValue) {
                value = Double.parseDouble(new String(valueBytes, 0, parseHead.bytesRead + position.bytesRead, StandardCharsets.UTF_8));
            } else {
                value = Double.parseDouble(new String(valueBytes, 0, position.bytesRead, StandardCharsets.UTF_8));
            }
            aggValue(aggMap, key, value);
        }
    }

    public static void main(String[] args) throws IOException {
        //Path measurements = Paths.get("/Users/franklyndsouza/src/github.com/damnMeddlingKid/1brc/src/test/resources/samples/measurements-3.txt");
        Path measurements = Paths.get(".", FILE);

        final int segmentSize = 4096;

        // The max mem we can map is 2GB
        final int readSize = segmentSize; //Integer.MAX_VALUE;

        HashMap<String, double[]> aggMap = new HashMap<>();
        byte[] segment = new byte[segmentSize];

        try(FileChannel fileChannel = (FileChannel) Files.newByteChannel(measurements, EnumSet.of(StandardOpenOption.READ))) {
            MappedFileReader reader = new MappedFileReader(measurements, 0, (int) Math.min(Integer.MAX_VALUE, fileChannel.size()));

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
