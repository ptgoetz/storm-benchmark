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
import org.apache.storm.utils.Utils;
import org.apache.storm.benchmark.util.TestUtils;
import org.testng.annotations.Test;

import static org.fest.assertions.api.Assertions.assertThat;

public class PageViewCountTest {

    @Test
    public void componentParallelismCouldBeSetThroughConfig() {
        StormBenchmark benchmark = new PageViewCount();
        Config config = new Config();
        config.put(PageViewCount.SPOUT_NUM, 3);
        config.put(PageViewCount.VIEW_NUM, 4);
        config.put(PageViewCount.COUNT_NUM, 5);

        StormTopology topology = benchmark.getTopology(config);
        assertThat(topology).isNotNull();
        TestUtils.verifyParallelism(Utils.getComponentCommon(topology, PageViewCount.SPOUT_ID), 3);
        TestUtils.verifyParallelism(Utils.getComponentCommon(topology, PageViewCount.VIEW_ID), 4);
        TestUtils.verifyParallelism(Utils.getComponentCommon(topology, PageViewCount.COUNT_ID), 5);
    }
}
