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
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.apache.storm.benchmark.api.Producer;
import org.apache.storm.benchmark.util.BenchmarkUtils;
import org.apache.storm.benchmark.util.KafkaUtils;
import org.apache.storm.kafka.bolt.KafkaBolt;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * KafkaProducer is itself a Storm benchmarks which consists of a KafkaProducerSpout and a KafkaBolt
 * Subclass could provide its own Spout (e.g. read from file, generate message randomly)
 */
public abstract class KafkaProducer extends Producer {

    public static final String SPOUT_ID = "spout";
    public static final String SPOUT_NUM = "producer.spout_num";
    public static final String BOLT_ID = "bolt";
    public static final String BOLT_NUM = "producer.bolt_num";
    public static final String BROKER_LIST = "broker.list";
    public static final String TOPIC = "topic";

    public static final int DEFAULT_SPOUT_NUM = 4;
    public static final int DEFAULT_BOLT_NUM = 4;
    protected final IRichBolt bolt = new KafkaBolt<String, String>();
    protected IRichSpout spout;

    @Override
    public StormTopology getTopology(Config config) {
        config.putAll(getKafkaConfig(config));

        final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
        final int boltNum = BenchmarkUtils.getInt(config, BOLT_NUM, DEFAULT_BOLT_NUM);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, spout, spoutNum);
        builder.setBolt(BOLT_ID, bolt, boltNum).localOrShuffleGrouping(SPOUT_ID);
        return builder.createTopology();
    }

    public IRichSpout getSpout() {
        return spout;
    }

    private Map getKafkaConfig(Map options) {
        Map kafkaConfig = new HashMap();
        Map brokerConfig = new HashMap();
        String brokers = (String) Utils.get(options, BROKER_LIST, "localhost:9092");
        String topic = (String) Utils.get(options, TOPIC, KafkaUtils.DEFAULT_TOPIC);
        brokerConfig.put("metadata.broker.list", brokers);
        brokerConfig.put("serializer.class", "kafka.serializer.StringEncoder");
        brokerConfig.put("key.serializer.class", "kafka.serializer.StringEncoder");
        brokerConfig.put("request.required.acks", "1");
//        kafkaConfig.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, brokerConfig);
        brokerConfig.put(KafkaBolt.TOPIC, topic);
        return kafkaConfig;
    }

    /**
     * KafkaProducerSpout generates source data for downstream KafkaBolt to
     * write into Kafka. The output fields consist of BOLT_KEY and BOLT_MESSAGE.
     * BOLT_KEY will decide the Kafka partition to write into and BOLT_MESSAGE the
     * actual message. Users set the number of partitions and by default messages will
     * be written into each partition in a round-robin way.
     */
    public static abstract class KafkaProducerSpout extends BaseRichSpout {

        private static final long serialVersionUID = -3823006007489002720L;
        private final Random random;
        protected SpoutOutputCollector collector;

        public KafkaProducerSpout() {
            random = new Random();
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // TODO fixme
//        declarer.declare(new Fields(KafkaBolt.BOLT_KEY, KafkaBolt.BOLT_MESSAGE));
        }

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        protected void nextMessage(String message) {
            collector.emit(new Values(random.nextInt() + "", message));
        }
    }
}
