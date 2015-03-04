# Storm Benchmark

Framework and set of test topologies for running various benchmarks against Apache Storm.

This is a work in progress. Minor bugs and annoyances are likely. Please report problems using github issues.


## Building

`mvn clean install`

## Running

__N.B.:__ The path to the `storm` executable must be included the $PATH environment variable.

```
usage: storm-benchmark [options] <benchmark_jar> <benchmark_config>
 -P,--poll <ms>     Metrics polling interval in ms.
 -p,--path <path>   Directory where reports will be saved.
 -t,--time <ms>     How long to run each benchmark in ms.
```

`java -jar target/storm-benchmark-0.1.0-jar-with-dependencies.jar target/storm-benchmark-0.1.0-jar-with-dependencies.jar benchmark_config.yaml`


## Benchmark Configuration
The benchmark config file is a YAML document which defines one or more individual benchmarks that are run in sequence as
a suite. Being able to define and run multiple configurations as a suite allows you to see at a glance the performance
impact of different configurations and tuning parameters.

Below is a sample configuration for running the word count benchmark twice with different configurations:

```yaml
# global report config
global:
  benchmark.poll.interval: 60000 # how often to sample metrics
  benchmark.runtime: 300000 # the total runtime of the benchmark
  benchmark.report.dir: /Users/tgoetz/tmp # directory where reports will be written

benchmarks:
  - benchmark.enabled: true
    benchmark.label: "core-wordcount-1"
    topology.class: org.apache.storm.benchmark.topologies.FileReadWordCount

    topology.config:
      #storm config params
      topology.name: "core-wordcount"
      topology.acker.executors: 2
      topology.max.spout.pending: 200
      topology.workers: 2

      # FileReadWordCount config params
      component.spout_num: 2
      component.split_bolt_num: 4
      component.count_bolt_num: 4

  - benchmark.enabled: true
    benchmark.label: "core-wordcount-2"
    topology.class: org.apache.storm.benchmark.topologies.FileReadWordCount

    topology.config:
      #storm config params
      topology.name: "core-wordcount"
      topology.acker.executors: 4
      topology.max.spout.pending: 1000
      topology.workers: 4

      # FileReadWordCount config params
      component.spout_num: 4
      component.split_bolt_num: 8
      component.count_bolt_num: 8
```


## Reporting
The results of each benchmark will be written to a CSV file in the directory identified by `benchmark.report.dir`. The
Storm topology configuration used by the benchmark will also by written as a YAML file.

Report files are created with the following naming convention:

`{benchmark.label}_metrics_{MM-dd-yyyy_HH.mm.ss}.csv`

Each report will contain a CSV header followed by the metrics samples for the benchmark. The sample interval can be
controlled via command line parameter or by setting `benchmark.poll.interval` in the benchmark config file.

### Report Fields
The following metrics are collected:

|Metric| Description|
|---------------------------|---|
|Time(s)| The time the metric was collected in seconds from the start|
|Total Slots| The total number of worker slots in the cluster |
|Used Slots| The number of worker slots used |
|Workers| The number of workers assigned to the topology |
|Tasks| The number of tasks assigned to the topology |
|Executors| The number of executors assigned to the topology |
|Transferred (messages)| The number of tuples transferred by the topology in the sample period|
|Throughput (messages/s)| The number of messages per second transferred by the topology |
|Throughput (MB/s)| The throughput in MB/s of the topology (Speed-of-Light benchmark only)|
|Spout Throughput (MB/s)| The throughput in MB/s of the topology (Speed-of-Light benchmark only) |
|Spout Executors| The number of executors assigned to spouts |
|Spout Transferred (messages)| The number of tuples transferred by spouts |
|Spout Acked | The number of tuple trees acked |
|Spout Failed| The number of tuple trees failed |
|Spout Throughput (messages/s)| Spout throughput in MB/s |
|Spout Avg. Complete Latency(ms)| Average complete latency |
|Spout Max. Complete Latency(ms)| Maximum complete latency |

## Benchmarking User-defined Topologies
TODO Document interface topologies must implement in order to be benchmarked.


## Using Benchmark "Producers"
"Producers" are special components that generate data for certain topologies. For example, benchmarks that use Storm's
Kafka spout rely on a Producer to generate and send data to Kafka.

__NOTE:__ Producers are not currently fully implemented.


## Acknowledgements

This project is a fork and refactoring of the following project:

https://github.com/manuzhang/storm-benchmark

The SOL (Speed of Light) benchmark is based on the following project from Yahoo!

https://github.com/yahoo/storm-perf-test
