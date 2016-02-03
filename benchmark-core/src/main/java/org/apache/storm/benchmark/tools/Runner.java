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

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.shade.org.yaml.snakeyaml.Yaml;
import org.apache.storm.utils.Utils;
import org.apache.log4j.Logger;
import org.apache.storm.benchmark.api.*;
import org.apache.storm.benchmark.metrics.IMetricsCollector;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Map;

/**
 * Runner is the main class of storm benchmark
 * It instantiates an IBenchmark from passed-in name and then run it
 */
public class Runner {
    private static final Logger LOG = Logger.getLogger(Runner.class);

    private static Config config = new Config();
    private static StormTopology topology;

    public static void main(String[] args) throws Exception {
        if (null == args || args.length < 1) {
            throw new IllegalArgumentException("no benchmark is set");
        }

        run(args[0]);
    }

    public static void run(String name)
            throws Exception {
        initConfig();
        IApplication app = getApplicationFromName(name);
        if (app instanceof Benchmark) {
            LOG.info("running benchmark " + name);
            runBenchmark((IBenchmark) app);
        } else if (app instanceof Producer) {
            LOG.info("running producer " + name);
            runProducer((IProducer) app);
        } else {
            throw new RuntimeException(name + " is neither benchmark nor producer");
        }
    }

    private static void initConfig() {
        Yaml yaml = new Yaml();
        File confFile = new File(System.getProperty("user.home"), ".storm/storm.yaml");
        if (confFile.exists()) {

            try {
                Map<String, Object> localConf = (Map<String, Object>) yaml.load(new FileInputStream(confFile));
                if(localConf != null) {
                    config.putAll(localConf);
                }
            } catch (FileNotFoundException e) {
                LOG.warn("Local storm config not found.", e);
            }

        } else {
            LOG.warn("Local storm config not found.");
        }

        config.putAll(Utils.readCommandLineOpts());

        if(!config.containsKey("nimbus.thrift.port")){
            config.put("nimbus.thrift.port", 6627);
        }
        if(!config.containsKey("storm.messaging.transport")){
            config.put("storm.messaging.transport", "org.apache.storm.messaging.netty.Context");
        }
        if(!config.containsKey("storm.thrift.transport")){
            config.put("storm.thrift.transport", "org.apache.storm.security.auth.SimpleTransportPlugin");
        }
        if(!config.containsKey("storm.nimbus.retry.times")){
            config.put("storm.nimbus.retry.times", 5);
        }
        if(!config.containsKey("storm.nimbus.retry.interval.millis")){
            config.put("storm.nimbus.retry.interval.millis", 2000);
        }
        if(!config.containsKey("storm.nimbus.retry.intervalceiling.millis")){
            config.put("storm.nimbus.retry.intervalceiling.millis", 60000);
        }
        if(!config.containsKey("nimbus.thrift.max_buffer_size")){
            config.put("nimbus.thrift.max_buffer_size", 1048576);
        }
    }

    public static IApplication getApplicationFromName(String name)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        return (IApplication) Class.forName(name).newInstance();
    }


    public static void runBenchmark(IBenchmark benchmark)
            throws Exception {
        runApplication(benchmark);
        IMetricsCollector collector = benchmark.getMetricsCollector(config);
        collector.run();
    }

    public static void runProducer(IProducer producer)
            throws Exception {
        runApplication(producer);
    }


    private static void runApplication(IApplication app)
            throws Exception {
        String name = (String) config.get(Config.TOPOLOGY_NAME);
        topology = app.getTopology(config);
        StormSubmitter.submitTopology(name, config, topology);
    }
}
