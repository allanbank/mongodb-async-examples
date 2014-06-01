/*
 *           Copyright 2014 - Allanbank Consulting, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ycsb;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ResultsParser provides the ability to parse the YCSB "stdout" files.
 * <p>
 * The parser assumes that the files use the file naming convention of:
 * <blockqoute>
 * 
 * <pre>
 * {workload}-{driver}-threads_{thread_count}-conns_{connection_count}-run.out
 * </pre>
 * 
 * </blockquote>
 * 
 * See the <a
 * href="http://www.allanbank.com/mongodb-async-driver/performance/ycsb.html"
 * >YCSB Test Results</a> for the script for running the tests.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ResultsParser {

    /** The set of workloads to process. */
    public static final List<String> WORKLOADS;

    /** The set of drivers to process. */
    public static final List<String> DRIVERS;

    /** The prefix for the data lines we are interested in. */
    public static final Map<String, String> PREFIXES;

    /** The values for the data lines we are interested in. */
    public static final Map<String, String> VALUES;

    static {
        final List<String> workloads = new ArrayList<>();
        for (char c = 'a'; c <= 'f'; ++c) {
            workloads.add("workload" + c);
        }
        WORKLOADS = Collections.unmodifiableList(workloads);

        final List<String> drivers = new ArrayList<>();
        drivers.add("mongodb");
        drivers.add("mongodb-async");
        DRIVERS = Collections.unmodifiableList(drivers);

        Map<String, String> prefixes = new HashMap<>();
        prefixes.put("[OVERALL], ", "Overall ");
        prefixes.put("[UPDATE], ", "Update ");
        prefixes.put("[READ], ", "Read ");
        prefixes.put("[INSERT], ", "Insert ");
        prefixes.put("[SCAN], ", "Scan ");
        prefixes.put("[READ-MODIFY-WRITE], ", "Read/Modify/Write ");
        PREFIXES = Collections.unmodifiableMap(prefixes);

        Map<String, String> values = new HashMap<>();
        values.put("RunTime(ms), ", "Runtime (ms)");
        values.put("Throughput(ops/sec), ", "Throughput (ops/sec)");
        values.put("Operations, ", "Operations");
        values.put("AverageLatency(us), ", "Average Latency (us)");
        values.put("MinLatency(us), ", "Min Latency (us)");
        values.put("MaxLatency(us), ", "Max Latency (us)");
        values.put("95thPercentileLatency(ms), ",
                "95th Percentile Latency (ms)");
        values.put("99thPercentileLatency(ms), ",
                "99th Percentile Latency (ms)");
        VALUES = Collections.unmodifiableMap(values);
    }

    /**
     * Runs the results parser.
     * 
     * @param args
     *            The command line arguments.
     * @throws IOException
     *             On a fatal failure.
     */
    public static void main(String[] args) throws IOException {

        if (args.length != 1) {
            System.out.println("Usage java " + ResultsParser.class.getName()
                    + " <results_directory>");
            return;
        }

        File dir = new File(args[0]);

        for (String workload : WORKLOADS) {
            File outFile = new File(dir, workload + ".csv");
            try (Writer fout = new FileWriter(outFile);
                    Writer out = new BufferedWriter(fout)) {

                boolean first = true;
                for (int connections = 1; connections < 25; ++connections) {
                    for (int threads = 1; threads < 150; ++threads) {

                        boolean canReadAll = true;
                        for (String driver : DRIVERS) {
                            File inFile = fileFor(dir, workload, driver,
                                    connections, threads);

                            canReadAll &= inFile.canRead();
                        }
                        if (canReadAll) {

                            if (first) {
                                for (String driver : DRIVERS) {
                                    File inFile = fileFor(dir, workload,
                                            driver, connections, threads);
                                    writeHeader(out, inFile);
                                    out.append(',');
                                }

                                out.append('\n');
                            }
                            first = false;

                            for (String driver : DRIVERS) {
                                out.append(workload).append(',');
                                out.append(driver).append(',');
                                out.append(String.valueOf(connections)).append(
                                        ',');
                                out.append(String.valueOf(threads)).append(',');

                                File inFile = fileFor(dir, workload, driver,
                                        connections, threads);
                                writeData(out, inFile);
                                out.append(',');
                            }
                            out.append('\n');
                        }
                    }
                }
            }
        }
    }

    /**
     * Reads the file a line at a time and writes the data we care about on a
     * single line.
     * 
     * @param out
     *            The file to write to.
     * @param inFile
     *            The file we will read.
     * @throws IOException
     *             On a failure to read the files.
     */
    private static void writeData(Writer out, File inFile) throws IOException {
        try (Reader fReader = new FileReader(inFile);
                BufferedReader reader = new BufferedReader(fReader)) {

            boolean first = true;
            String line = null;
            while ((line = reader.readLine()) != null) {
                String prefix = null;
                for (Map.Entry<String, String> entry : PREFIXES.entrySet()) {
                    if (line.startsWith(entry.getKey())) {
                        line = line.substring(entry.getKey().length());

                        prefix = entry.getValue();
                    }
                }

                if (prefix != null) {
                    for (Map.Entry<String, String> entry : VALUES.entrySet()) {
                        if (line.startsWith(entry.getKey())) {
                            line = line.substring(entry.getKey().length());

                            if (!first) {
                                out.append(",");
                            }
                            else {
                                first = false;
                            }

                            if (entry.getKey().startsWith("95th")
                                    || entry.getKey().startsWith("99th")) {
                                // One of the percentiles. Multiply by 1000 to
                                // convert from ms --> us.
                                try {
                                    int value = Integer.parseInt(line);

                                    line = String.valueOf(value * 1000);
                                }
                                catch (NumberFormatException nfe) {
                                    // Skip it.
                                }
                            }

                            out.append(line);
                        }
                    }
                }
            }
        }
    }

    /**
     * Creates the file for the specified test criteria.
     * 
     * @param dir
     *            The directory to find the rest results.
     * @param workload
     *            The workload name.
     * @param driver
     *            The driver used.
     * @param connections
     *            The number of connections used.
     * @param threads
     *            The number of threads used.
     * @return The file (which may or may not exist).
     */
    private static File fileFor(File dir, String workload, String driver,
            int connections, int threads) {
        File inFile = new File(dir, workload + "-" + driver + "-threads_"
                + threads + "-conns_" + connections + "-run.out");
        return inFile;
    }

    /**
     * Reads the file a line at a time and writes the header for the data we
     * write.
     * 
     * @param out
     *            The file to write to.
     * @param inFile
     *            The file we will read.
     * @throws IOException
     *             On a failure to read the files.
     */
    private static void writeHeader(Writer out, File inFile) throws IOException {
        try (Reader fReader = new FileReader(inFile);
                BufferedReader reader = new BufferedReader(fReader)) {

            out.append("Workload,Driver,Connections,Threads,");

            boolean first = true;
            String line = null;
            while ((line = reader.readLine()) != null) {
                String prefix = null;
                for (Map.Entry<String, String> entry : PREFIXES.entrySet()) {
                    if (line.startsWith(entry.getKey())) {
                        line = line.substring(entry.getKey().length());

                        prefix = entry.getValue();
                    }
                }

                if (prefix != null) {
                    for (Map.Entry<String, String> entry : VALUES.entrySet()) {
                        if (line.startsWith(entry.getKey())) {
                            line = line.substring(entry.getKey().length());

                            if (!first) {
                                out.append(",");
                            }
                            else {
                                first = false;
                            }
                            out.append(prefix).append(entry.getValue());
                        }
                    }
                }
            }
        }
    }
}
