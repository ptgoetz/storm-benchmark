package org.apache.storm.benchmark;

public final class BenchmarkConstants {
    public static final String CONF_FILE_FORMAT = "%s/%s_metrics_%s.yaml";
    public static final String DATA_FILE_FORMAT = "%s/%s_metrics_%s.csv";
    public static final String DATE_FORMAT = "MM-dd-yyyy_HH.mm.ss";

    public static final String METRICS_POLL_INTERVAL = "benchmark.poll.interval";
    public static final String METRICS_TOTAL_TIME = "benchmark.runtime";
    public static final String METRICS_PATH = "benchmark.report.dir";

    public static final int DEFAULT_POLL_INTERVAL = 30 * 1000; // 30 secs
    public static final int DEFAULT_TOTAL_TIME = 5 * 60 * 1000; // 5 mins
    public static final String DEFAULT_PATH = "/root/";
}
