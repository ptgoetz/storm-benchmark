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

package org.apache.storm.benchmark.producers.kafka;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;
import org.apache.storm.benchmark.util.FileReader;
import org.junit.Ignore;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class FileReadKafkaProducerTest {

    private static final Map ANY_CONF = new HashMap();

//    @Test
//    @Ignore
//    public void spoutShouldBeKafkaFileReadSpout() {
//        KafkaProducer producer = new FileReadKafkaProducer();
//        producer.getTopology(new Config());
//        assertThat(producer.getSpout()).isInstanceOf(FileReadKafkaProducer.FileReadSpout.class);
//    }

    @Test
    public void nextTupleShouldEmitNextLineOfFile() throws Exception {
        FileReader reader = mock(FileReader.class);
        String message = "line";
        FileReadKafkaProducer.FileReadSpout spout = new FileReadKafkaProducer.FileReadSpout(reader);
        TopologyContext context = mock(TopologyContext.class);
        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);

        when(reader.nextLine()).thenReturn(message);

        spout.open(ANY_CONF, context, collector);
        spout.nextTuple();

        verify(collector, times(1)).emit(any(Values.class));
    }
}
