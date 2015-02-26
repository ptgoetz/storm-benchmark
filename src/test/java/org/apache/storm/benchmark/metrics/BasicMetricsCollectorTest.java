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
import backtype.storm.generated.StormTopology;
import com.google.common.collect.Sets;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.PrintWriter;
import java.util.Set;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class BasicMetricsCollectorTest {

    private static final Config config = new Config();
    private StormTopology topology;

    @BeforeMethod
    public void setUp() {
        topology = mock(StormTopology.class);
    }

    @Test(dataProvider = "getSupervisorMetrics")
    public void testCollectSupervisorStats(Set<IMetricsCollector.MetricsItem> items, boolean expected) {
        BasicMetricsCollector collector = new BasicMetricsCollector(config, topology, items);
        assertThat(collector.collectSupervisorStats).isEqualTo(expected);
    }

    @DataProvider
    private Object[][] getSupervisorMetrics() {
        Set<IMetricsCollector.MetricsItem> allButSupervisorStats = Sets.newHashSet(IMetricsCollector.MetricsItem.values());
        allButSupervisorStats.remove(IMetricsCollector.MetricsItem.ALL);
        allButSupervisorStats.remove(IMetricsCollector.MetricsItem.SUPERVISOR_STATS);
        return new Object[][]{
                {Sets.newHashSet(IMetricsCollector.MetricsItem.SUPERVISOR_STATS), true},
                {Sets.newHashSet(IMetricsCollector.MetricsItem.ALL), true},
                {allButSupervisorStats, false},
                {Sets.newHashSet(IMetricsCollector.MetricsItem.SUPERVISOR_STATS, IMetricsCollector.MetricsItem.TOPOLOGY_STATS), true}
        };
    }

    @Test(dataProvider = "getTopologyMetrics")
    public void testCollectTopologyStats(Set<IMetricsCollector.MetricsItem> items, boolean expected) {
        BasicMetricsCollector collector = new BasicMetricsCollector(config, topology, items);
        assertThat(collector.collectTopologyStats).isEqualTo(expected);
    }

    @DataProvider
    public Object[][] getTopologyMetrics() {
        Set<IMetricsCollector.MetricsItem> allButTopologyStats = Sets.newHashSet(IMetricsCollector.MetricsItem.values());
        allButTopologyStats.remove(IMetricsCollector.MetricsItem.ALL);
        allButTopologyStats.remove(IMetricsCollector.MetricsItem.TOPOLOGY_STATS);
        return new Object[][]{
                {Sets.newHashSet(IMetricsCollector.MetricsItem.TOPOLOGY_STATS), true},
                {Sets.newHashSet(IMetricsCollector.MetricsItem.ALL), true},
                {allButTopologyStats, false},
                {Sets.newHashSet(IMetricsCollector.MetricsItem.TOPOLOGY_STATS, IMetricsCollector.MetricsItem.SPOUT_LATENCY), true}
        };
    }

    @Test(dataProvider = "getExecutorMetrics")
    public void testCollectExecutorStats(Set<IMetricsCollector.MetricsItem> items, boolean expected) {
        BasicMetricsCollector collector = new BasicMetricsCollector(config, topology, items);
        assertThat(collector.collectExecutorStats).isEqualTo(expected);
    }

    @DataProvider
    public Object[][] getExecutorMetrics() {
        return new Object[][]{
                {Sets.newHashSet(IMetricsCollector.MetricsItem.THROUGHPUT, IMetricsCollector.MetricsItem.SUPERVISOR_STATS), true},
                {Sets.newHashSet(IMetricsCollector.MetricsItem.THROUGHPUT_IN_MB, IMetricsCollector.MetricsItem.TOPOLOGY_STATS), true},
                {Sets.newHashSet(IMetricsCollector.MetricsItem.SPOUT_THROUGHPUT), true},
                {Sets.newHashSet(IMetricsCollector.MetricsItem.SPOUT_LATENCY), true},
                {Sets.newHashSet(IMetricsCollector.MetricsItem.SUPERVISOR_STATS, IMetricsCollector.MetricsItem.TOPOLOGY_STATS), false},
                {Sets.newHashSet(IMetricsCollector.MetricsItem.ALL), true},
        };
    }

    @Test(dataProvider = "getThroughputMetrics")
    public void testCollectThroughput(Set<IMetricsCollector.MetricsItem> items, boolean expected) {
        BasicMetricsCollector collector = new BasicMetricsCollector(config, topology, items);
        assertThat(collector.collectThroughput).isEqualTo(expected);
    }

    @DataProvider
    public Object[][] getThroughputMetrics() {
        Set<IMetricsCollector.MetricsItem> allButThroughput = Sets.newHashSet(IMetricsCollector.MetricsItem.values());
        allButThroughput.remove(IMetricsCollector.MetricsItem.ALL);
        allButThroughput.remove(IMetricsCollector.MetricsItem.THROUGHPUT);
        return new Object[][]{
                {Sets.newHashSet(IMetricsCollector.MetricsItem.THROUGHPUT), true},
                {Sets.newHashSet(IMetricsCollector.MetricsItem.ALL), true},
                {allButThroughput, false},
                {Sets.newHashSet(IMetricsCollector.MetricsItem.TOPOLOGY_STATS, IMetricsCollector.MetricsItem.THROUGHPUT), true}
        };
    }

    @Test(dataProvider = "getThroughputMBMetrics")
    public void testCollectThroughputMB(Set<IMetricsCollector.MetricsItem> items, boolean expected) {
        BasicMetricsCollector collector = new BasicMetricsCollector(config, topology, items);
        assertThat(collector.collectThroughputMB).isEqualTo(expected);
    }

    @DataProvider
    public Object[][] getThroughputMBMetrics() {
        Set<IMetricsCollector.MetricsItem> allButThroughputMB = Sets.newHashSet(IMetricsCollector.MetricsItem.values());
        allButThroughputMB.remove(IMetricsCollector.MetricsItem.ALL);
        allButThroughputMB.remove(IMetricsCollector.MetricsItem.THROUGHPUT_IN_MB);
        return new Object[][]{
                {Sets.newHashSet(IMetricsCollector.MetricsItem.THROUGHPUT_IN_MB), true},
                {Sets.newHashSet(IMetricsCollector.MetricsItem.ALL), true},
                {allButThroughputMB, false},
                {Sets.newHashSet(IMetricsCollector.MetricsItem.SUPERVISOR_STATS, IMetricsCollector.MetricsItem.THROUGHPUT_IN_MB), true}
        };
    }

    @Test(dataProvider = "getSpoutThroughputMetrics")
    public void testCollectSpoutThroughput(Set<IMetricsCollector.MetricsItem> items, boolean expected) {
        BasicMetricsCollector collector = new BasicMetricsCollector(config, topology, items);
        assertThat(collector.collectSpoutThroughput).isEqualTo(expected);
    }

    @DataProvider
    public Object[][] getSpoutThroughputMetrics() {
        Set<IMetricsCollector.MetricsItem> allButSpoutThroughput = Sets.newHashSet(IMetricsCollector.MetricsItem.values());
        allButSpoutThroughput.remove(IMetricsCollector.MetricsItem.ALL);
        allButSpoutThroughput.remove(IMetricsCollector.MetricsItem.SPOUT_THROUGHPUT);
        return new Object[][]{
                {Sets.newHashSet(IMetricsCollector.MetricsItem.SPOUT_THROUGHPUT), true},
                {Sets.newHashSet(IMetricsCollector.MetricsItem.ALL), true},
                {allButSpoutThroughput, false},
                {Sets.newHashSet(IMetricsCollector.MetricsItem.THROUGHPUT, IMetricsCollector.MetricsItem.SPOUT_THROUGHPUT), true}
        };
    }

    @Test(dataProvider = "getSpoutLatencyMetrics")
    public void testCollectSpoutLatency(Set<IMetricsCollector.MetricsItem> items, boolean expected) {
        BasicMetricsCollector collector = new BasicMetricsCollector(config, topology, items);
        assertThat(collector.collectSpoutLatency).isEqualTo(expected);
    }

    @DataProvider
    public Object[][] getSpoutLatencyMetrics() {
        Set<IMetricsCollector.MetricsItem> allButSpoutLatency = Sets.newHashSet(IMetricsCollector.MetricsItem.values());
        allButSpoutLatency.remove(IMetricsCollector.MetricsItem.ALL);
        allButSpoutLatency.remove(IMetricsCollector.MetricsItem.SPOUT_LATENCY);
        return new Object[][]{
                {Sets.newHashSet(IMetricsCollector.MetricsItem.SPOUT_LATENCY), true},
                {Sets.newHashSet(IMetricsCollector.MetricsItem.ALL), true},
                {allButSpoutLatency, false},
                {Sets.newHashSet(IMetricsCollector.MetricsItem.THROUGHPUT_IN_MB, IMetricsCollector.MetricsItem.SPOUT_LATENCY), true}
        };

    }


    @Test(dataProvider = "getMetricsItems")
    public void testWriteHeaders(Set<IMetricsCollector.MetricsItem> items, Set<String> expected) {
        BasicMetricsCollector collector = new BasicMetricsCollector(config, topology, items);
        PrintWriter writer = mock(PrintWriter.class);

        collector.writeHeader(writer);

        verify(writer, times(1)).println(anyString());
        verify(writer, times(1)).flush();
        for (String h : expected) {
            collector.header.contains(h);
        }

    }

    @DataProvider
    public Object[][] getMetricsItems() {
        Set<String> supervisor = Sets.newHashSet(
                BasicMetricsCollector.TIME,
                BasicMetricsCollector.TOTAL_SLOTS,
                BasicMetricsCollector.USED_SLOTS);
        Set<String> topology = Sets.newHashSet(
                BasicMetricsCollector.TIME,
                BasicMetricsCollector.EXECUTORS,
                BasicMetricsCollector.WORKERS,
                BasicMetricsCollector.TASKS
        );
        Set<String> throughput = Sets.newHashSet(
                BasicMetricsCollector.TIME,
                BasicMetricsCollector.TRANSFERRED,
                BasicMetricsCollector.THROUGHPUT
        );
        Set<String> throughputMB = Sets.newHashSet(
                BasicMetricsCollector.TIME,
                BasicMetricsCollector.THROUGHPUT_MB,
                BasicMetricsCollector.SPOUT_THROUGHPUT_MB
        );
        Set<String> spoutThroughput = Sets.newHashSet(
                BasicMetricsCollector.TIME,
                BasicMetricsCollector.SPOUT_ACKED,
                BasicMetricsCollector.SPOUT_TRANSFERRED,
                BasicMetricsCollector.SPOUT_EXECUTORS,
                BasicMetricsCollector.SPOUT_THROUGHPUT
        );
        Set<String> spoutLatency = Sets.newHashSet(
                BasicMetricsCollector.TIME,
                BasicMetricsCollector.SPOUT_AVG_COMPLETE_LATENCY,
                BasicMetricsCollector.SPOUT_MAX_COMPLETE_LATENCY
        );
        return new Object[][]{
                {Sets.newHashSet(IMetricsCollector.MetricsItem.SUPERVISOR_STATS), supervisor},
                {Sets.newHashSet(IMetricsCollector.MetricsItem.TOPOLOGY_STATS), topology},
                {Sets.newHashSet(IMetricsCollector.MetricsItem.THROUGHPUT), throughput},
                {Sets.newHashSet(IMetricsCollector.MetricsItem.THROUGHPUT_IN_MB), throughputMB},
                {Sets.newHashSet(IMetricsCollector.MetricsItem.SPOUT_THROUGHPUT), spoutThroughput},
                {Sets.newHashSet(IMetricsCollector.MetricsItem.SPOUT_LATENCY), spoutLatency}
        };
    }

}
