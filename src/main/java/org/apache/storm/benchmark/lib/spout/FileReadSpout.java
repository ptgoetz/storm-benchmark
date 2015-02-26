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

package org.apache.storm.benchmark.lib.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import org.apache.storm.benchmark.util.FileReader;

import java.util.Map;

public class FileReadSpout extends BaseRichSpout {
    public static final String DEFAULT_FILE = "/resources/A_Tale_of_Two_City.txt";
    //  public static final boolean DEFAULT_ACK = false;
    public static final String FIELDS = "sentence";
    private static final Logger LOG = Logger.getLogger(FileReadSpout.class);
    private static final long serialVersionUID = -2582705611472467172L;
    private transient FileReader reader;
    private String file;
    private boolean ackEnabled = true;
    private SpoutOutputCollector collector;

    private long count = 0;


    public FileReadSpout() {
        this(DEFAULT_FILE);
    }


    public FileReadSpout(String file) {
        this.file = file;
    }

    // used for testing
    FileReadSpout(FileReader reader) {
        this.reader = reader;
    }

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
        Object ackObj = conf.get("topology.acker.executors");
        if (ackObj != null && ackObj.equals(0)) {
            this.ackEnabled = false;
        }
        // for tests, reader will not be null
        if (this.reader == null) {
            this.reader = new FileReader(this.file);
        }
    }

    @Override
    public void nextTuple() {
        if (ackEnabled) {
            collector.emit(new Values(reader.nextLine()), count);
            count++;
        } else {
            collector.emit(new Values(reader.nextLine()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FIELDS));
    }
}
