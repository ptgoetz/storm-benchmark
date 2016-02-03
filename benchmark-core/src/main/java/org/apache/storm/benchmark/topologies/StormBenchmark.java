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

package org.apache.storm.benchmark.topologies;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import com.google.common.collect.Sets;
import org.apache.log4j.Logger;
import org.apache.storm.benchmark.api.Benchmark;
import org.apache.storm.benchmark.metrics.BasicMetricsCollector;
import org.apache.storm.benchmark.metrics.IMetricsCollector;

import java.util.Set;

import static org.apache.storm.benchmark.metrics.IMetricsCollector.MetricsItem;

public abstract class StormBenchmark extends Benchmark {

    public static final String DEFAULT_TOPOLOGY_NAME = "benchmark";
    private static final Logger LOG = Logger.getLogger(StormBenchmark.class);

    @Override
    public IMetricsCollector getMetricsCollector(Config config) {

        Set<MetricsItem> items = Sets.newHashSet(
                MetricsItem.SUPERVISOR_STATS,
                MetricsItem.TOPOLOGY_STATS,
                MetricsItem.THROUGHPUT,
                MetricsItem.SPOUT_THROUGHPUT,
                MetricsItem.SPOUT_LATENCY
        );
        return new BasicMetricsCollector(config, items);
    }


}
