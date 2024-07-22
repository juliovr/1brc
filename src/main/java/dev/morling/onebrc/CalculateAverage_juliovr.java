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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        long start = System.nanoTime();

        Map<String, ResultRow> result = new TreeMap<>();

        // Map<String, MeasurementAggregator> aggregators = new HashMap<>(10_000);

//        try (FileChannel fileChannel = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
//            long filesize = fileChannel.size();
//            MemorySegment segment = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, filesize, Arena.global());
//            long address = segment.address();
//
//            Unsafe unsafe = Scanner.getUnsafe();
//
//            // MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, filesize);
//            // int size = 1 << 30;
//            // ByteBuffer dst = ByteBuffer.allocate(size);
//            // mmap.read(dst);
//
//            byte[] bytes = new byte[64];
//            for (int i = 0; i < bytes.length; i++) {
//                bytes[i] = unsafe.getByte(address + i);
//            }
//
//            String s = new String(bytes);
//
//            long i = 0;
//            long lines = 0;
//            while (i < filesize) {
//                // char c = (char)buffer.get(i);
//                long c = unsafe.getLong(address + (i * 8));
//                if (c == '\n') {
//                    lines++;
//
//                    if (lines % 50_000_000 == 0) {
//                        System.out.println("Lines = " + lines);
//                    }
//                }
//
//                i++;
//            }
//
//            System.out.println("Lines = " + lines);
//        }

        int cores = Runtime.getRuntime().availableProcessors();
//        ExecutorService executor = Executors.newFixedThreadPool(cores);

        System.out.println("Number of cores = " + cores);

        try (FileChannel fileChannel = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
            long fileSize = fileChannel.size();

            long chunkSize = fileSize / cores;

            MemorySegment segment = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global());
            long address = segment.address();
            long end = address + fileSize;

            // Unsafe could be unnecessary, because the values can be access directly by the segment
//            byte b = segment.get(ValueLayout.OfInt.JAVA_BYTE, 0);
            Unsafe unsafe = Scanner.getUnsafe();

            long lines = 0;
            while (address < end) {
                long l = unsafe.getLong(address);
                address += 8;

                long maskNewLine = 0x0A0A0A0A0A0A0A0AL;
                long masked = l ^ maskNewLine;
                long hasNewLine = (masked - 0x0101010101010101L) & (~l) & (0x8080808080808080L);

                lines += countSetBits(hasNewLine);
//                byte[] bytes = new byte[4096];
//                for (int i = 0; i < bytes.length; i++) {
//                    bytes[i] = unsafe.getByte(address++);
//                }
//
//                String s = new String(bytes, StandardCharsets.UTF_8);
//                System.out.println(s);
            }

            System.out.println("Lines = " + lines);
        }

//        long lines = 1_000_000_000;
//        long linesPerThread = lines / cores;
//
//        Future<?>[] futures = new Future[cores];
//        try (FileChannel fileChannel = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
//            long fileSize = fileChannel.size();
//            long chunkSize = fileSize / cores;
//
//            long lastChunkSize = (fileSize - (chunkSize * (cores - 1)));
//
//            for (int i = 0; i < futures.length; i++) {
//                long chunkStart = (chunkSize * i);
//                int threadId = i;
//
//                futures[i] = executor.submit(() -> {
//                    Map<String, MeasurementAggregator> aggregators = new HashMap<>(10_000);
//
//                    long chunkEnd = chunkStart + chunkSize;
//                    long size = (threadId == futures.length - 1) ? lastChunkSize : chunkSize;
//                    ByteBuffer buffer = ByteBuffer.allocate((int)size);
//                    fileChannel.read(buffer);
//
////                    String s = new String(buffer.array(), StandardCharsets.UTF_8);
//                    System.out.printf("[%d] = %d\n", threadId, threadId);
//
////                    while (currentLine < lineEnd) {
////                        String line = raf.readLine();
////                        String[] splitted = line.split(";");
////                        String station = splitted[0];
////                        double value = Double.parseDouble(splitted[1]);
////                        MeasurementAggregator aggregator = aggregators.get(station);
////                        if (aggregator == null) {
////                            aggregator = new MeasurementAggregator();
////                            aggregators.put(station, aggregator);
////                        }
////
////                        aggregator.min = Math.min(aggregator.min, value);
////                        aggregator.max = Math.max(aggregator.max, value);
////                        aggregator.sum += value;
////                        aggregator.count++;
////
////                        currentLine++;
////                        if (currentLine % 1_000_000 == 0) {
////                            System.out.println("Line = " + currentLine);
////                        }
////                    }
//
//                    return aggregators;
//                });
//            }
//        }

//        Map<String, MeasurementAggregator> aggregators = new HashMap<>(10_000);
//        for (int i = 0; i < futures.length; i++) {
//            Map<String, MeasurementAggregator> aggThreadResult = (Map<String, MeasurementAggregator>)futures[i].get();
//            for (Map.Entry<String, MeasurementAggregator> entry : aggThreadResult.entrySet()) {
//                String station = entry.getKey();
//                MeasurementAggregator agg = entry.getValue();
//
//                MeasurementAggregator finalAggregator = aggregators.get(station);
//                if (finalAggregator == null) {
//                    finalAggregator = new MeasurementAggregator();
//                    aggregators.put(station, finalAggregator);
//                }
//
//                finalAggregator.min = Math.min(finalAggregator.min, agg.min);
//                finalAggregator.max = Math.max(finalAggregator.max, agg.max);
//                finalAggregator.sum += finalAggregator.sum;
//                finalAggregator.count += finalAggregator.count;
//            }
//        }
//
//        for (Map.Entry<String, MeasurementAggregator> entry : aggregators.entrySet()) {
//            String station = entry.getKey();
//            MeasurementAggregator agg = entry.getValue();
//
//            result.put(station, new ResultRow(agg.min, agg.sum / agg.count, agg.max));
//        }
//
//        System.out.println(result);

        long finish = System.nanoTime();
        long timeElapsed = finish - start;

        System.out.println("Time elapsed = " + (timeElapsed / 1_000_000_000) + " seconds");
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
