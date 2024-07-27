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

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

public class CalculateAverage_juliovr {

    private static final int MAX_STATIONS = 10_000;
//    private static final String FILE = "./measurements_test.txt";
    private static final String FILE = "./measurements.txt";

    private static class Result {
        private short min = Short.MAX_VALUE;
        private short max = Short.MIN_VALUE;
        private double sum;
        private int count;

        public String toString() {
            return round((double)min) + "/" + round((double)sum / (double)count) + "/" + round((double)max);
        }

        private double round(double value) {
            return Math.round(value) / 10.0;
        }
    }

    // TODO: generate entire table for long values
    private static final int[] BITS_TABLE = { 0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4 };

    private static int countSetBits(long l) {
        int count = 0;
        for (int i = 0; i < 16; i++) {
            int value = (int)(l & 0xF);
            count += BITS_TABLE[value];
            l >>= 4;
        }

        return count;
    }

    // Unsafe could be unnecessary, because the values can be access directly by the segment
//            byte b = segment.get(ValueLayout.OfInt.JAVA_BYTE, 0);
    private static final Unsafe unsafe = initUnsafe();

    private static Unsafe initUnsafe() {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe)f.get(Unsafe.class);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        long start = System.nanoTime();

//        int nThreads = 2;
        int nThreads = Runtime.getRuntime().availableProcessors();
        System.out.println("Number of threads = " + nThreads);

        try (FileChannel fileChannel = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
            long fileSize = fileChannel.size();

            MemorySegment segment = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global());
            long address = segment.address();
            long fileSizeAddress = address + fileSize;

            long chunkSize = fileSize / nThreads;

            final Thread[] threads = new Thread[nThreads];
            final long[] lines = new long[nThreads];

            long currentAddress = address;
            Map<String, Result>[] aggregators = new Map[nThreads];
            for (int i = 0; i < nThreads; i++) {
                if (currentAddress >= fileSizeAddress) {
                    break;
                }

                long addressStart = currentAddress;
                long addressEnd = (i == nThreads - 1) ? fileSizeAddress : nextNewLine(addressStart + chunkSize);

                currentAddress = addressEnd + 1;

                final int threadId = i;
                threads[i] = new Thread(() -> {
                    Map<String, Result> result = new HashMap<>(MAX_STATIONS);

                    long countLinesThread = process(addressStart, addressEnd, result);

                    lines[threadId] = countLinesThread;
                    aggregators[threadId] = result;
                });
                threads[i].start();
            }

            for (int i = 0; i < nThreads; i++) {
                if (threads[i] != null) {
                    threads[i].join();
                }
            }

            // Merge results
            Map<String, Result> result = new TreeMap<>();
            for (int i = 0; i < aggregators.length; i++) {
                if (aggregators[i] == null) {
                    continue;
                }

                for (Map.Entry<String, Result> entry : aggregators[i].entrySet()) {
                    String stationName = entry.getKey();
                    Result r = entry.getValue();
                    Result existing = result.get(stationName);
                    if (existing == null) {
                        result.put(stationName, r);
                    } else {
                        if (r.min < existing.min) {
                            existing.min = r.min;
                        }
                        if (r.max > existing.max) {
                            existing.max = r.max;
                        }
                        existing.sum += r.sum;
                        existing.count += r.count;
                    }
                }
            }

            long totalLines = 0;
            for (int i = 0; i < lines.length; i++) {
                totalLines += lines[i];
            }
            System.out.println("Lines = " + totalLines);

            System.out.println(result);
        }


        long finish = System.nanoTime();
        long timeElapsed = finish - start;

        System.out.println("Time elapsed = " + (timeElapsed / 1_000_000_000) + " seconds");
        System.out.println("Time elapsed = " + timeElapsed + " nanoseconds");
    }



    /*
    pos new line        char pos    trailing zeros  index (trailing zeros / 8 or shifting by 3)
    0x8000000000000000  8           63              7
    0x0080000000000000  7           55              6
    0x0000800000000000  6           47              5
    0x0000008000000000  5           39              4
    0x0000000080000000  4           31              3
    0x0000000000800000  3           23              2
    0x0000000000008000  2           15              1
    0x0000000000000080  1           7               0
     */
    private static long nextNewLine(long address) {
        while (true) {
            long value = unsafe.getLong(address);

            long mask = 0x0A0A0A0A0A0A0A0AL;
            long masked = value ^ mask;
            long posNewLine = (masked - 0x0101010101010101L) & (~value) & (0x8080808080808080L);

            if (posNewLine != 0) {
                address += Long.numberOfTrailingZeros(posNewLine) >>> 3; // Divide by 3 to get the index of the char
                break;
            }

            address += 8;
        }

        return address;
    }

    private static long nextSemicolon(long address) {
        while (true) {
            long value = unsafe.getLong(address);

            long mask = 0x3B3B3B3B3B3B3B3BL;
            long masked = value ^ mask;
            long posSemicolon = (masked - 0x0101010101010101L) & (~value) & (0x8080808080808080L);

            if (posSemicolon != 0) {
                address += Long.numberOfTrailingZeros(posSemicolon) >>> 3; // Divide by 3 to get the index of the char
                break;
            }

            address += 8;
        }

        return address;
    }

    private static long process(long addressStart, long addressEnd, Map<String, Result> accumulative) {
        long currentAddress = addressStart;
        long countLinesThread = 0;


        byte[] bytes = new byte[100];
        byte[] bytesDouble = new byte[4];
        while (currentAddress < addressEnd) {
            long posNewLine = nextNewLine(currentAddress);
            long posSemicolon = nextSemicolon(currentAddress);

            countLinesThread++;

            int stationNameLength = (int) (posSemicolon - currentAddress);
            int valueLength = (int)(posNewLine - posSemicolon - 1);
            for (int i = 0; i < stationNameLength; i++) {
                bytes[i] = unsafe.getByte(currentAddress + i);
            }

            String stationName = new String(bytes, 0, stationNameLength, StandardCharsets.UTF_8);

            int index = 0;
            for (int i = 0; i < valueLength; i++) {
                byte aByte = unsafe.getByte(posSemicolon + 1 + i);
                if (aByte == '.') {
                    continue;
                }
                bytesDouble[index++] = aByte;
            }

            // TODO: optimize this
            short value = Short.parseShort(new String(bytesDouble, 0, valueLength - 1));

            Result r = accumulative.get(stationName);
            if (r == null) {
                r = new Result();

                accumulative.put(stationName, r);
            }

            if (value < r.min) {
                r.min = value;
            }
            if (value > r.max) {
                r.max = value;
            }
            r.sum += value;
            r.count++;

            currentAddress = (posNewLine + 1);
        }

        return countLinesThread;
    }

}
