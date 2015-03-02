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
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;
import org.apache.storm.benchmark.BenchmarkConstants;
import org.apache.storm.benchmark.topologies.StormBenchmark;
import org.apache.storm.benchmark.util.BenchmarkUtils;

import java.io.PrintWriter;
import java.util.Map;
import java.util.TreeMap;

public class MetricsCollectorConfig {
    private static final Logger LOG = Logger.getLogger(MetricsCollectorConfig.class);

    // storm configuration
    public final Config stormConfig;
    // storm topology name
    public final String name;
    // benchmark label
    public final String label;
    // How often should metrics be collected
    public final int pollInterval;
    // How long should the benchmark run for
    public final int totalTime;
    // metrics file path
    public final String path;
    // warmup delay in milliseconds
    public static Long warmupDelay = 0L;

    public MetricsCollectorConfig(Config stormConfig) {
        this.stormConfig = stormConfig;
        String labelStr = (String) stormConfig.get("benchmark.label");
        this.name = (String) Utils.get(
                stormConfig, Config.TOPOLOGY_NAME, StormBenchmark.DEFAULT_TOPOLOGY_NAME);
        if (labelStr == null) {
            LOG.warn("'benchmark.label' not found in config. Defaulting to topology name");
            labelStr = this.name;
        }
        this.label = labelStr;

        this.warmupDelay = BenchmarkUtils.getLong(stormConfig, "benchmark.warmup.time", 0);

        pollInterval = BenchmarkUtils.getInt(
                stormConfig, BenchmarkConstants.METRICS_POLL_INTERVAL, BenchmarkConstants.DEFAULT_POLL_INTERVAL);
        totalTime = BenchmarkUtils.getInt(
                stormConfig, BenchmarkConstants.METRICS_TOTAL_TIME, BenchmarkConstants.DEFAULT_TOTAL_TIME);
        path = (String) Utils.get(stormConfig, BenchmarkConstants.METRICS_PATH, BenchmarkConstants.DEFAULT_PATH);
    }


    public void writeStormConfig(PrintWriter writer) {
        LOG.info("writing out storm config into .yaml file");
        if (writer != null) {
            Map sorted = new TreeMap();
            sorted.putAll(stormConfig);
            for (Object key : sorted.keySet()) {
                writer.println(key + ": " + stormConfig.get(key));
            }
            writer.flush();
        }
    }
}
