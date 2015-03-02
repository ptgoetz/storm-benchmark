/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */
package org.apache.storm.benchmark.tools;

import org.apache.commons.cli.*;
import org.apache.storm.benchmark.BenchmarkConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Map;

import static org.apache.storm.benchmark.BenchmarkConstants.*;


public class BenchmarkRunner {
    public static final Logger LOG = LoggerFactory.getLogger(BenchmarkRunner.class);

    public static void main(String[] args) throws Exception {

        // TODO actually do something with the options, for now they are ignored
        Options options = new Options();

        Option runTimeOpt = OptionBuilder.hasArgs(1)
                .withArgName("ms")
                .withLongOpt("time")
                .withDescription("How long to run each benchmark in ms.")
                .create("t");
        options.addOption(runTimeOpt);

        Option pollTimeOpt = OptionBuilder.hasArgs(1)
                .withArgName("ms")
                .withLongOpt("poll")
                .withDescription("Metrics polling interval in ms.")
                .create("P");
        options.addOption(pollTimeOpt);

        Option reportPathOpt = OptionBuilder.hasArgs(1)
                .withArgName("path")
                .withLongOpt("path")
                .withDescription("Directory where reports will be saved.")
                .create("p");
        options.addOption(reportPathOpt);


        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.getArgs().length != 1) {
            usage(options);
            System.exit(1);
        }



        String[] argArray = cmd.getArgs();
        runBenchmarks(cmd);
        LOG.info("Benchmark run complete.");
    }

    private static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("storm-benchmark [options] <benchmark file>", options);
    }


    public static void runBenchmarks(CommandLine cmd) throws Exception {
        Yaml yaml = new Yaml();

        FileInputStream in = new FileInputStream((String) cmd.getArgList().get(0));

        Map<String, Object> suiteConf = (Map) yaml.load(in);
        in.close();

        Map<String, Object> reportConfig = (Map<String, Object>)suiteConf.get("global");
        if(cmd.hasOption("t")){
            reportConfig.put("benchmark.runtime", Long.parseLong(cmd.getOptionValue("t")));
        }
        if(cmd.hasOption("P")){
            reportConfig.put("benchmark.poll.interval", Long.parseLong(cmd.getOptionValue("P")));
        }
        if(cmd.hasOption("p")){
            reportConfig.put("benchmark.report.dir", cmd.getOptionValue("p"));
        }
        ArrayList<Map<String, Object>> benchmarks = (ArrayList<Map<String, Object>>) suiteConf.get("benchmarks");

        LOG.info("Found " + benchmarks.size() + " benchmarks.");
        int benchmarkCount = 0;
        for (Map<String, Object> config : benchmarks) {
            if ((Boolean) config.get("benchmark.enabled") == true) {
                LOG.info("Will run benchmark: {}", config.get("benchmark.label"));
                benchmarkCount++;
            }
        }
        LOG.info("Running {} of {} benchmarks.", benchmarkCount, benchmarks.size());
        for (Map<String, Object> config : benchmarks) {
            if ((Boolean) config.get("benchmark.enabled") == true) {
                config.putAll(reportConfig);
                runTest((String) config.get("topology.class"), config);
            }
        }
    }

    private static void runTest(String topologyClass, Map<String, Object> benchmarkConfig) throws Exception {
        ArrayList<String> command = new ArrayList<String>();
        command.add("storm");
        command.add("jar");
        //FIXME NOW!!!!!!!
        command.add("target/storm-benchmark-0.1.0-jar-with-dependencies.jar");
        command.add("org.apache.storm.benchmark.tools.Runner");
        command.add(topologyClass);
        addBenchmarkCommandOpts(command, benchmarkConfig);

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);

        final Process proc = pb.start();
        Thread t = new Thread(new StreamRedirect(proc.getInputStream()));
        t.start();

        LOG.info("started process");
        int exitVal = proc.waitFor();
        LOG.info("exitVal=" + exitVal);

        killTopology(benchmarkConfig);
    }


    private static void killTopology(Map<String, Object> benchmarkConfig) throws Exception {

        Map<String, Object> topConfig = (Map<String, Object>) benchmarkConfig.get("topology.config");
//        String timeoutStr = (String) topConfig.get("topology.message.timeout.secs");
        String topologyName = (String) topConfig.get("topology.name");

        // TODO are we really that concerned about waiting for processing to complete? we could just shoot it right away.
//        int timeoutSecs = 30;
//        if(timeoutStr != null){
//            try{
//                timeoutSecs = Integer.parseInt(timeoutStr);
//            } catch(NumberFormatException e){
//            }
//        }

        String[] command = new String[]{"storm", "kill", topologyName, "-w", "5"};

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);

        final Process proc = pb.start();
        Thread t = new Thread(new StreamRedirect(proc.getInputStream()));
        t.start();

        LOG.info("Killing topology: " + topologyName);
        int exitVal = proc.waitFor();
        LOG.info("exitVal=" + exitVal);

        if (exitVal == 0) {
            LOG.info("Waiting for topology to complete.");
            Thread.sleep(5000);
        }
    }

    private static void addBenchmarkCommandOpts(ArrayList<String> cmd, Map<String, Object> config) {
        String[] opts = new String[]{METRICS_POLL_INTERVAL, METRICS_TOTAL_TIME, METRICS_PATH, "benchmark.label"};
        for (String s : opts) {
            cmd.add("-c");
            cmd.add(s + "=" + config.get(s));
        }

        Map<String, Object> topConfig = (Map<String, Object>) config.get("topology.config");
        for (String s : topConfig.keySet()) {
            cmd.add("-c");
            cmd.add(s + "=" + topConfig.get(s));
        }
    }


    private static class StreamRedirect implements Runnable {
        private InputStream in;

        public StreamRedirect(InputStream in) {
            this.in = in;
        }

        @Override
        public void run() {
            try {
                LOG.info("Running");
                int i = -1;
                while ((i = this.in.read()) != -1) {
                    System.out.write(i);
                }
                this.in.close();
                LOG.info("Stream reader closed.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}