package org.apache.storm.benchmark.metrics;

import org.apache.storm.Config;
import org.apache.storm.generated.*;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class MetricsSample {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsSample.class);

    private long sampleTime = -1;
    private long totalTransferred = 0l;
    private long totalEmitted = 0l;
    private long totalAcked = 0l;
    private long totalFailed = 0l;

    private double totalLatency;

    private long spoutEmitted = 0l;
    private long spoutTransferred = 0l;
    private int spoutExecutors = 0;

    private int numSupervisors = 0;
    private int numWorkers = 0;
    private int numTasks = 0;
    private int numExecutors = 0;

    private int totalSlots = 0;
    private int usedSlots = 0;

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.put("nimbus.thrift.port", 6627);
        config.put("storm.messaging.transport", "org.apache.storm.messaging.netty.Context");
        config.put("storm.thrift.transport", "org.apache.storm.security.auth.SimpleTransportPlugin");
        config.put("storm.nimbus.retry.times", 5);
        config.put("storm.nimbus.retry.interval.millis", 2000);
        config.put("storm.nimbus.retry.intervalceiling.millis", 60000);
        config.put("nimbus.thrift.max_buffer_size", 1048576);
        ArrayList<String> seeds = new ArrayList<>();
        seeds.add("localhost");
        config.put("nimbus.seeds", seeds);
        Nimbus.Client nimbus = NimbusClient.getConfiguredClient(config).getClient();

        factory(nimbus, "trident-wordcount");

    }

    public static MetricsSample factory(Nimbus.Client client, String topologyName) throws Exception {
        LOG.info("************ Sampling Metrics *****************");
        MetricsSample ret = new MetricsSample();

        ClusterSummary clusterSummary = client.getClusterInfo();
        LOG.info("*** Read Cluster Summary **");
        List<SupervisorSummary> supervisors = clusterSummary.get_supervisors();
        int totalSlots = 0;
        int usedSlots = 0;
        for(SupervisorSummary supervisor : supervisors){
            totalSlots += supervisor.get_num_workers();
            usedSlots += supervisor.get_num_used_workers();
        }
        int freeSlots = totalSlots - usedSlots;

        TopologySummary topSummary = getTopologySummary(clusterSummary, topologyName);
        int topologyExecutors = topSummary.get_num_executors();
        int topologyWorkers = topSummary.get_num_workers();
        int topologyTasks = topSummary.get_num_tasks();
        LOG.info("getting topology info for {}", topologyName);


        TopologyInfo topInfo = client.getTopologyInfo(topSummary.get_id());

        List<ExecutorSummary> executorSummaries = topInfo.get_executors();

        // total
        long totalTransferred = 0l;
        long totalEmitted = 0l;
        long totalAcked = 0l;
        long totalFailed = 0l;

        // number of spout executors
        int spoutExecCount = 0;
        double spoutLatencySum = 0.0;

        long spoutEmitted = 0l;
        long spoutTransferred = 0l;

        // Executor summaries
        for(ExecutorSummary executorSummary : executorSummaries){


            ExecutorStats execuatorStats = executorSummary.get_stats();
            if(execuatorStats == null){
                continue;
            }
            // doesn't appear to return what you would expect...
            // seems more like # of tasks/executors
//            LOG.debug(String.format("\t\temitted: %s, transferred: %s",
//                    execuatorStats.get_emitted_size(),
//                    execuatorStats.get_transferred_size()
//            ));

            ExecutorSpecificStats executorSpecificStats = execuatorStats.get_specific();
            if(executorSpecificStats == null){
                // bail out
                continue;
            }

            // transferred totals
            Map<String,Map<String,Long>> transferred = execuatorStats.get_transferred();
            Map<String, Long> txMap = transferred.get(":all-time");
            if(txMap == null){
                continue;
            }
            for(String key : txMap.keySet()){
                // todo, ignore the master batch coordinator ?
                if(!Utils.isSystemId(key)){
                    Long count = txMap.get(key);
                    totalTransferred += count;
                    if(executorSpecificStats.is_set_spout()){
                        spoutTransferred += count;
                    }
                }
            }
            LOG.info("*** read transferred totals...");

            // emitted totals
            Map<String,Map<String,Long>> emitted = execuatorStats.get_emitted();
            if(emitted != null) {
                LOG.info("emitted: {}", emitted);
                Map<String, Long> emMap = emitted.get(":all-time");

                for (String key : emMap.keySet()) {
                    if (!Utils.isSystemId(key)) {
                        Long count = emMap.get(key);
                        totalEmitted += count;
                        if (executorSpecificStats.is_set_spout()) {
                            spoutEmitted += count;
                        }
                    }
                }
            }
            LOG.info("*** read emitted totals...");

            // we found a spout
            if(executorSpecificStats.isSet(2)) { // spout
                LOG.info("Found a spout.");

                SpoutStats spoutStats = executorSpecificStats.get_spout();
                Map<String, Long> acked = spoutStats.get_acked().get(":all-time");
                if(acked != null){
                    for(String key : acked.keySet()) {
                        totalAcked += acked.get(key);
                    }
                }

                Map<String, Long> failed = spoutStats.get_failed().get(":all-time");
                if(failed != null){
                    for(String key : failed.keySet()) {
                        totalFailed += failed.get(key);
                    }
                }

                Double total = 0d;
                Map<String, Double> vals = spoutStats.get_complete_ms_avg().get(":all-time");
                for(String key : vals.keySet()){
                    total += vals.get(key);
                }
                Double latency = total / vals.size();

                spoutExecCount++;
                spoutLatencySum += latency;
            }

            // we found a bolt
            if(executorSpecificStats.isSet(1)) {
                LOG.info("ignoring bolt status.");
            }

        } // end executor summary

        LOG.info("====== RESULTS ======");
        LOG.info("Total emitted: {}", totalEmitted);
        LOG.info("Total transferred: {}", totalTransferred);
        LOG.info("Total avg latency: {}", spoutLatencySum / spoutExecCount);
        LOG.info("Spout emitted: {}", spoutEmitted);
        LOG.info("Spout transferred: {}", spoutTransferred);
        LOG.info("Total Acked: {}", totalAcked);
        LOG.info("Total Failed: {}", totalFailed);

        ret.totalEmitted = totalEmitted;
        ret.totalTransferred = totalTransferred;
        ret.totalAcked  = totalAcked;
        ret.totalFailed = totalFailed;
        ret.totalLatency = spoutLatencySum/spoutExecCount;
        ret.spoutEmitted = spoutEmitted;
        ret.spoutTransferred = spoutTransferred;
        ret.sampleTime = System.currentTimeMillis();
        ret.numSupervisors = clusterSummary.get_supervisors_size();
        ret.totalSlots = totalSlots;
        ret.usedSlots = usedSlots;
        ret.numWorkers = topologyWorkers;
        ret.numExecutors = topologyExecutors;
        ret.numTasks = topologyTasks;
        ret.spoutExecutors = spoutExecCount;
        return ret;


    }

    public static TopologySummary getTopologySummary(ClusterSummary cs, String name) {
        for (TopologySummary ts : cs.get_topologies()) {
            if (name.equals(ts.get_name())) {
                LOG.info("*** returning topology summary");
                return ts;
            }
        }
        return null;
    }



    // getters
    public long getSampleTime() {
        return sampleTime;
    }

    public long getTotalTransferred() {
        return totalTransferred;
    }

    public long getTotalEmitted() {
        return totalEmitted;
    }

    public long getTotalAcked() {
        return totalAcked;
    }

    public long getTotalFailed() {
        return totalFailed;
    }

    public double getTotalLatency() {
        return totalLatency;
    }

    public long getSpoutEmitted() {
        return spoutEmitted;
    }

    public long getSpoutTransferred() {
        return spoutTransferred;
    }

    public int getNumSupervisors() {
        return numSupervisors;
    }

    public int getNumWorkers() {
        return numWorkers;
    }

    public int getNumTasks() {
        return numTasks;
    }

    public int getTotalSlots() {
        return totalSlots;
    }

    public int getSpoutExecutors(){
        return this.spoutExecutors;
    }

    public int getNumExecutors() {
        return this.numExecutors;
    }

    public int getUsedSlots() {
        return this.usedSlots;
    }

}
