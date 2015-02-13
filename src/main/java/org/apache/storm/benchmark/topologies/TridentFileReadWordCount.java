package org.apache.storm.benchmark.topologies;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.apache.storm.benchmark.lib.spout.TridentFileReadSpout;
import org.apache.storm.benchmark.util.BenchmarkUtils;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;

public class TridentFileReadWordCount extends StormBenchmark {

    public static final String SPOUT_ID = "spout";
    public static final String SPOUT_NUM = "component.spout_num";
//    public static final String SPLIT_NUM = "component.split_bolt_num";
    public static final String COUNT_NUM = "component.count_bolt_num";
    public static final String BATCH_SIZE = "trident.spout.batch.size";

    public static final int DEFAULT_SPOUT_NUM = 8;
//    public static final int DEFAULT_SPLIT_BOLT_NUM = 4;
    public static final int DEFAULT_COUNT_BOLT_NUM = 4;
    public static final int DEFAULT_BATCH_SIZE = 1000;


    @Override
    public StormTopology getTopology(Config config) {
        final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
//        final int splitNum = BenchmarkUtils.getInt(config, SPLIT_NUM, DEFAULT_SPLIT_BOLT_NUM);
        final int countNum = BenchmarkUtils.getInt(config, COUNT_NUM, DEFAULT_COUNT_BOLT_NUM);
        final int batchSize = BenchmarkUtils.getInt(config, BATCH_SIZE, DEFAULT_BATCH_SIZE);


        TridentTopology topology = new TridentTopology();
        TridentFileReadSpout spout = new TridentFileReadSpout(batchSize);

        TridentState wordCounts = topology.newStream("spout1", spout)
                .parallelismHint(spoutNum)
                .each(new Fields("sentence"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(countNum);

        return topology.build();
    }
}
