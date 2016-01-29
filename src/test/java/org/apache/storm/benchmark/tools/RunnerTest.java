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


import org.apache.storm.benchmark.api.IApplication;
import org.junit.Ignore;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.junit.Assert.assertTrue;


public class RunnerTest {

//    @Test(dataProvider = "getValidNames")
//    @Ignore
//    public void getBenchmarkFromValidName(String validName) throws Exception {
//        assertTrue(Runner.getApplicationFromName(validName) instanceof IApplication);
//    }

    @Test(dataProvider = "getInValidNames", expectedExceptions = ClassNotFoundException.class)
    public void throwsExceptionFromInvalidName(String invalidName)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        Runner.getApplicationFromName(invalidName);
    }

    @DataProvider
    private Object[][] getValidNames() {
        return new String[][]{
                {"org.apache.storm.benchmark.topologies.FileReadWordCount"},
                {"org.apache.storm.benchmark.topologies.SOL"},
                {"org.apache.storm.benchmark.topologies.Grep"},
                {"org.apache.storm.benchmark.topologies.PageViewCount"},
                {"org.apache.storm.benchmark.topologies.UniqueVisitor"},
                {"org.apache.storm.benchmark.topologies.KafkaWordCount"},
                {"org.apache.storm.benchmark.topologies.DRPC"},
                {"org.apache.storm.benchmark.topologies.RollingCount"},
                {"org.apache.storm.benchmark.topologies.SOL"},
                {"org.apache.storm.benchmark.topologies.TridentWordCount"},
                {"org.apache.storm.benchmark.producers.kafka.FileReadKafkaProducer"},
                {"org.apache.storm.benchmark.producers.kafka.PageViewKafkaProducer"}
        };
    }

    @DataProvider
    private Object[][] getInValidNames() {
        return new String[][]{{"foo"}, {"bar"}};
    }
}
