package storm.benchmark.tools;

import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Map;

public class BenchmarkRunner {
    public static void main(String[] args) throws Exception {
        Yaml yaml = new Yaml();
        FileInputStream in = new FileInputStream("resources/benchmark_suite.yaml");

        Map<String, Object> suiteConf = (Map) yaml.load(in);
        in.close();

        ArrayList<Map<String, Object>> benchmarks = (ArrayList<Map<String, Object>>)suiteConf.get("benchmark-suite");
        System.out.println("Found " + benchmarks.size() + " benchmarks.");
        for(Map<String, Object> config : benchmarks){
            if((Boolean)config.get("benchmark.enabled") == true)
            runTest((String)config.get("topology.class"), config);
        }

        System.out.println();
    }


    public static void runSuite(Map<String, Object> suite) {

    }


    public static void runBenchmark(){

    }

    private static void runTest(String topologyClass, Map<String, Object> benchmarkConfig) throws Exception {
        ArrayList<String> command = new ArrayList<String>();
        command.add("storm");
        command.add("jar");
        command.add("target/storm-benchmark-0.1.0-jar-with-dependencies.jar");
        command.add("storm.benchmark.tools.Runner");
        command.add(topologyClass);
        addBenchmarkCommandOpts(command, benchmarkConfig);

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);

        final Process proc = pb.start();
        Thread t = new Thread(new StreamRedirect(proc.getInputStream()));
        t.start();

        System.out.println("started process");
        int exitVal = proc.waitFor();
        System.out.println("exitVal=" + exitVal);

        killTopology(benchmarkConfig);
    }


    private static void killTopology(Map<String, Object> benchmarkConfig) throws Exception {

        Map<String, Object> topConfig = (Map<String, Object>)benchmarkConfig.get("topology.config");
//        String timeoutStr = (String) topConfig.get("topology.message.timeout.secs");
        String topologyName = (String) topConfig.get("topology.name");

        // TODO are we really that concerned about waiting for processing to complete? we could just shoot it right away.
//        int timeoutSecs = 30;
//        if(timeoutStr != null){
//            try{
//                timeoutSecs = Integer.parseInt(timeoutStr);
//            } catch(NumberFormatException e){
//            }
//        }

        String[] command = new String[]{"storm", "kill", topologyName, "-w", "5"};

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);

        final Process proc = pb.start();
        Thread t = new Thread(new StreamRedirect(proc.getInputStream()));
        t.start();

        System.out.println("Killing topology: " + topologyName);
        int exitVal = proc.waitFor();
        System.out.println("exitVal=" + exitVal);

        if(exitVal == 0){
            System.out.println("Waiting for topology to complete.");
            Thread.sleep(5000);
        }
    }

    private static void addBenchmarkCommandOpts(ArrayList<String> cmd, Map<String, Object> config){
        String[] opts = new String[]{"metrics.poll", "metrics.time", "metrics.path", "benchmark.label"};
        for(String s : opts){
            cmd.add("-c");
            cmd.add(s + "=" + config.get(s));
        }

        Map<String, Object> topConfig = (Map<String, Object>)config.get("topology.config");
        for(String s : topConfig.keySet()){
            cmd.add("-c");
            cmd.add(s + "=" + topConfig.get(s));
        }
    }




    private static class StreamRedirect implements Runnable {
        private InputStream in;

        public StreamRedirect(InputStream in){
            this.in = in;
        }

        @Override
        public void run() {
            try {
                System.out.println("Running");
                int i = -1;
                while ((i = this.in.read()) != -1) {
                    System.out.write(i);
                }
                this.in.close();
                System.out.println("Stream reader closed.");
            } catch(Exception e){
                e.printStackTrace();
            }
        }
    }
}