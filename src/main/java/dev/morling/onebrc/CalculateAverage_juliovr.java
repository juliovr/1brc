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
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

public class CalculateAverage_juliovr {

    private static final String FILE = "./measurements.txt";

    private static record ResultRow(double min, double mean, double max) {

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
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
    private static final Unsafe unsafe = Scanner.getUnsafe();

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        long start = System.nanoTime();

        Map<String, ResultRow> result = new TreeMap<>();

        int nThreads = Runtime.getRuntime().availableProcessors();
//        int nThreads = 2;
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
            for (int i = 0; i < nThreads; i++) {
                long addressStart = currentAddress;
                long addressEnd = (i == nThreads - 1) ? fileSizeAddress : nextNewLine(addressStart + chunkSize);

                currentAddress = addressEnd + 1;

                final int threadId = i;
                threads[i] = new Thread(() -> {
                    long countLinesThread = process(addressStart, addressEnd);

                    lines[threadId] = countLinesThread;
                });
                threads[i].start();
            }

            for (int i = 0; i < nThreads; i++) {
                threads[i].join();
            }

            long totalLines = 0;
            for (int i = 0; i < lines.length; i++) {
                totalLines += lines[i];
            }
            System.out.println("Lines = " + totalLines);
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

    private static long process(long addressStart, long addressEnd) {
        long currentAddress = addressStart;
        long countLinesThread = 0;

        while (currentAddress < addressEnd) {
//            long value = unsafe.getLong(currentAddress);

            long posNewLine = nextNewLine(currentAddress);
//            long posSemicolon = nextSemicolon(currentAddress);

            countLinesThread++;

//            byte[] bytes = new byte[100];
//            int length = (int)(posSemicolon - currentAddress);
//            for (int i = 0; i < length; i++) {
//                bytes[i] = unsafe.getByte(currentAddress + i);
//            }

//            String s = new String(bytes, 0, length, StandardCharsets.UTF_8);
//
//            System.out.println(s);

            currentAddress = (posNewLine + 1);
        }

        return countLinesThread;
    }

    private static class Scanner {

        private static final Unsafe UNSAFE = initUnsafe();

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

        public static Unsafe getUnsafe() {
            return UNSAFE;
        }

    }
}
