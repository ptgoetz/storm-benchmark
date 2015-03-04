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

package org.apache.storm.benchmark.metrics;

import backtype.storm.Config;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;
import org.apache.storm.benchmark.lib.spout.RandomMessageSpout;
import org.apache.storm.benchmark.util.BenchmarkUtils;
import org.apache.storm.benchmark.util.FileUtils;

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.storm.benchmark.BenchmarkConstants.*;

public class BasicMetricsCollector implements IMetricsCollector {
    /* headers */
    public static final String TIME = "time(s)";
    public static final String TIME_FORMAT = "%d";
    public static final String TOTAL_SLOTS = "total_slots";
    public static final String USED_SLOTS = "used_slots";
    public static final String WORKERS = "workers";
    public static final String TASKS = "tasks";
    public static final String EXECUTORS = "executors";
    public static final String TRANSFERRED = "transferred (messages)";
    public static final String THROUGHPUT = "throughput (messages/s)";
    public static final String THROUGHPUT_MB = "throughput (MB/s)";
    public static final String THROUGHPUT_MB_FORMAT = "%.1f";
    public static final String SPOUT_EXECUTORS = "spout_executors";
    public static final String SPOUT_TRANSFERRED = "spout_transferred (messages)";
    public static final String SPOUT_ACKED = "spout_acked (messages)";
    public static final String SPOUT_THROUGHPUT = "spout_throughput (messages/s)";
    public static final String SPOUT_THROUGHPUT_MB = "spout_throughput (MB/s)";
    public static final String SPOUT_THROUGHPUT_MB_FORMAT = "%.1f";
    public static final String SPOUT_AVG_COMPLETE_LATENCY = "spout_avg_complete_latency(ms)";
    public static final String SPOUT_AVG_LATENCY_FORMAT = "%.1f";
    public static final String SPOUT_MAX_COMPLETE_LATENCY = "spout_max_complete_latency(ms)";
    public static final String SPOUT_MAX_LATENCY_FORMAT = "%.1f";
    private static final Logger LOG = Logger.getLogger(BasicMetricsCollector.class);
    final MetricsCollectorConfig config;
//    final StormTopology topology;
    final Set<String> header = new LinkedHashSet<String>();
    final Map<String, String> metrics = new HashMap<String, String>();
    final int msgSize;
    final boolean collectSupervisorStats;
    final boolean collectTopologyStats;
    final boolean collectExecutorStats;
    final boolean collectThroughput;
    final boolean collectThroughputMB;
    final boolean collectSpoutThroughput;
    final boolean collectSpoutLatency;

    private MetricsSample lastSample;
    private MetricsSample curSample;
    private double maxLatency = 0;

    public BasicMetricsCollector(Config stormConfig, Set<MetricsItem> items) {
        this.config = new MetricsCollectorConfig(stormConfig);
//        this.topology = topology;
        collectSupervisorStats = collectSupervisorStats(items);
        collectTopologyStats = collectTopologyStats(items);
        collectExecutorStats = collectExecutorStats(items);
        collectThroughput = collectThroughput(items);
        collectThroughputMB = collectThroughputMB(items);
        collectSpoutThroughput = collectSpoutThroughput(items);
        collectSpoutLatency = collectSpoutLatency(items);
        msgSize = collectThroughputMB ?
                BenchmarkUtils.getInt(stormConfig, RandomMessageSpout.MESSAGE_SIZE,
                        RandomMessageSpout.DEFAULT_MESSAGE_SIZE) : 0;
    }

    @Override
    public void run() {
        LOG.info(String.format("Waiting %s ms. for topology warm-up...", config.warmupDelay));
        Utils.sleep(config.warmupDelay);

        Nimbus.Client client = getNimbusClient(config.stormConfig);

        boolean first = true;

        Date date = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
        long now = date.getTime();
        long endTime = now + config.totalTime;
        MetricsState state = new MetricsState();
        state.startTime = now;
        state.lastTime = now;

        final String path = config.path;
        final String name = config.label;
        final String confFile = String.format(
                CONF_FILE_FORMAT, path, name, dateFormat.format(date));
        final String dataFile = String.format(
                DATA_FILE_FORMAT, path, name, dateFormat.format(date));
        PrintWriter confWriter = FileUtils.createFileWriter(path, confFile);
        PrintWriter dataWriter = FileUtils.createFileWriter(path, dataFile);
        config.writeStormConfig(confWriter);
        writeHeader(dataWriter);

        try {
            do {
                if(!first) {
                    Utils.sleep(config.pollInterval);
                    this.lastSample = this.curSample;
                    this.curSample = MetricsSample.factory(client, config.name);
                    pollNimbus(state, dataWriter);
                } else {
                    LOG.info("Getting baseline metrics sample.");
                    this.curSample = MetricsSample.factory(client, config.name);
                    first = false;
                }
                now = System.currentTimeMillis();

            } while (now < endTime);
        } catch (Exception e) {
            LOG.error("storm metrics failed! ", e);
        } finally {
            dataWriter.close();
            confWriter.close();
        }
    }

    public Nimbus.Client getNimbusClient(Config stormConfig) {
        return NimbusClient.getConfiguredClient(stormConfig).getClient();
    }


    boolean pollNimbus(MetricsState state, PrintWriter writer)
            throws Exception {
        final String name = config.name;

        if (collectSupervisorStats) {
            updateSupervisorStats();
        }

        if (collectTopologyStats) {
            updateTopologyStats(state);
        }

        if (collectExecutorStats) {
            updateExecutorStats(state);
        }

        writeLine(writer);
        state.lastTime = System.currentTimeMillis();

        return true;
    }

    void updateTopologyStats(MetricsState state) {
        long timeTotal = this.curSample.getSampleTime() - state.startTime;
        int numWorkers = this.curSample.getNumWorkers();
        int numExecutors = this.curSample.getNumExecutors();
        int numTasks = this.curSample.getNumTasks();
        metrics.put(TIME, String.format(TIME_FORMAT, timeTotal / 1000));
        metrics.put(WORKERS, Integer.toString(numWorkers));
        metrics.put(EXECUTORS, Integer.toString(numExecutors));
        metrics.put(TASKS, Integer.toString(numTasks));
    }

    void updateSupervisorStats() {
        metrics.put(TOTAL_SLOTS, Integer.toString(this.curSample.getTotalSlots()));
        metrics.put(USED_SLOTS, Integer.toString(this.curSample.getUsedSlots()));
    }

    void updateExecutorStats(MetricsState state) {
        long timeDiff = this.curSample.getSampleTime() - this.lastSample.getSampleTime();
        long transferredDiff = this.curSample.getTotalTransferred() - this.lastSample.getTotalTransferred();
        long throughput = transferredDiff / (timeDiff / 1000);
        double throughputMB = (throughput * msgSize) / (1024 * 1024);

        long spoutDiff = this.curSample.getSpoutTransferred() - this.lastSample.getSpoutTransferred();
        long spoutAckedDiff = this.curSample.getTotalAcked() - this.lastSample.getTotalAcked();
        long spoutThroughput = spoutDiff / (timeDiff / 1000);
        double spoutThroughputMB = (spoutThroughput * msgSize) / (1024 * 1024);

        if (collectThroughput) {
            metrics.put(TRANSFERRED, Long.toString(transferredDiff));
            metrics.put(THROUGHPUT, Long.toString(throughput));
        }
        if (collectThroughputMB) {
            metrics.put(THROUGHPUT_MB, String.format(THROUGHPUT_MB_FORMAT, throughputMB));
        }
        if (collectSpoutThroughput) {

            metrics.put(SPOUT_EXECUTORS, Integer.toString(this.curSample.getSpoutExecutors()));
            metrics.put(SPOUT_TRANSFERRED, Long.toString(spoutDiff));
            metrics.put(SPOUT_ACKED, Long.toString(spoutAckedDiff));
            metrics.put(SPOUT_THROUGHPUT, Long.toString(spoutThroughput));
        }
        if (collectThroughputMB) {
            metrics.put(SPOUT_THROUGHPUT_MB,
                    String.format(SPOUT_THROUGHPUT_MB_FORMAT, spoutThroughputMB));

        }


        if (collectSpoutLatency) {
            double latency = this.curSample.getTotalLatency();
            if(latency > this.maxLatency){
                this.maxLatency = latency;
            }
            metrics.put(SPOUT_AVG_COMPLETE_LATENCY,
                    String.format(SPOUT_AVG_LATENCY_FORMAT, latency));
            metrics.put(SPOUT_MAX_COMPLETE_LATENCY,
                    String.format(SPOUT_MAX_LATENCY_FORMAT, this.maxLatency));

        }
    }




    void writeHeader(PrintWriter writer) {
        header.add(TIME);

        if (collectSupervisorStats) {
            header.add(TOTAL_SLOTS);
            header.add(USED_SLOTS);
        }

        if (collectTopologyStats) {
            header.add(WORKERS);
            header.add(TASKS);
            header.add(EXECUTORS);
        }

        if (collectThroughput) {
            header.add(TRANSFERRED);
            header.add(THROUGHPUT);
        }

        if (collectThroughputMB) {
            header.add(THROUGHPUT_MB);
            header.add(SPOUT_THROUGHPUT_MB);
        }


        if (collectSpoutThroughput) {
            header.add(SPOUT_EXECUTORS);
            header.add(SPOUT_TRANSFERRED);
            header.add(SPOUT_ACKED);
            header.add(SPOUT_THROUGHPUT);
        }


        if (collectSpoutLatency) {
            header.add(SPOUT_AVG_COMPLETE_LATENCY);
            header.add(SPOUT_MAX_COMPLETE_LATENCY);
        }

        String str = Utils.join(header, ",");
        LOG.info("writing out metrics headers [" + str + "] into .csv file");
        writer.println(str);
        writer.flush();
    }

    void writeLine(PrintWriter writer) {
        List<String> line = new LinkedList<String>();
        for (String h : header) {
            line.add(metrics.get(h));
        }
        String str = Utils.join(line, ",");
        LOG.info("writing out metrics results [" + str + "] into .csv file");
        writer.println(Utils.join(line, ","));
        writer.flush();
    }


    boolean collectSupervisorStats(Set<MetricsItem> items) {
        return items.contains(MetricsItem.ALL) ||
                items.contains(MetricsItem.SUPERVISOR_STATS);
    }

    boolean collectTopologyStats(Set<MetricsItem> items) {
        return items.contains(MetricsItem.ALL) ||
                items.contains(MetricsItem.TOPOLOGY_STATS);
    }

    boolean collectExecutorStats(Set<MetricsItem> items) {
        return items.contains(MetricsItem.ALL) ||
                items.contains(MetricsItem.THROUGHPUT) ||
                items.contains(MetricsItem.THROUGHPUT_IN_MB) ||
                items.contains(MetricsItem.SPOUT_THROUGHPUT) ||
                items.contains(MetricsItem.SPOUT_LATENCY);
    }

    boolean collectThroughput(Set<MetricsItem> items) {
        return items.contains(MetricsItem.ALL) ||
                items.contains(MetricsItem.THROUGHPUT);
    }

    boolean collectThroughputMB(Set<MetricsItem> items) {
        return items.contains(MetricsItem.ALL) ||
                items.contains(MetricsItem.THROUGHPUT_IN_MB);
    }

    boolean collectSpoutThroughput(Set<MetricsItem> items) {
        return items.contains(MetricsItem.ALL) ||
                items.contains(MetricsItem.SPOUT_THROUGHPUT);
    }

    boolean collectSpoutLatency(Set<MetricsItem> items) {
        return items.contains(MetricsItem.ALL) ||
                items.contains(MetricsItem.SPOUT_LATENCY);
    }


    static class MetricsState {
//        long overallTransferred = 0;
//        long spoutTransferred = 0;
        long startTime = 0;
        long lastTime = 0;
    }
}
