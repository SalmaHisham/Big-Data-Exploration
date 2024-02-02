import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

public class Duration_Calculation {

    public static void main(String[] args) throws Exception {
        // Path to the input file containing IP addresses and timestamps
        String inputFile = "/home/salma/Desktop/Master/big_data/labs/Lab5/Lab5_Material/ipAndTimestamp.csv";

        // Create a Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Read the data from the source data and assign timestamps with watermarks
        DataStream<Tuple2<String, String>> inputStream = env.addSource(new SourceFunction<Tuple2<String, String>>() {
            @Override
            public void run(SourceContext<Tuple2<String, String>> sourceContext) throws Exception {
                BufferedReader reader;
                try {
                    // Open the file for reading
                    reader = new BufferedReader(new FileReader(inputFile));

                    // Read each line of the file
                    String line = reader.readLine();
                    while (line != null) {
                        if (line.split(",").length > 1) {
                            // Parse the line into a Tuple2 (IP address, timestamp) and emit
                            sourceContext.collect(new Tuple2<>(
                                    line.split(",")[0],
                                    line.split(",")[1]
                            ));
                        }
                        // Read the next line
                        line = reader.readLine();
                    }
                    // Close the reader after processing the file
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void cancel() {
                // Implementation of cancel is not necessary for this example
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, String>>forBoundedOutOfOrderness(Duration.ofMillis(1000))
                .withTimestampAssigner((event, ts) -> Instant.parse(event.f1).toEpochMilli()));

        // Calculate session duration for each distinct IP
        inputStream
                .keyBy(e -> e.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .process(new ProcessWindowFunction<Tuple2<String, String>, Object, String, TimeWindow>() {
                    // Set to keep track of distinct IP addresses
                    Set<String> distinctIPs = new HashSet<>();

                    @Override
                    public void process(String key, ProcessWindowFunction<Tuple2<String, String>, Object, String, TimeWindow>.Context context,
                                        Iterable<Tuple2<String, String>> iterable, Collector<Object> collector) throws Exception {
                        // If IP is encountered for the first time, calculate session duration and emit
                        if (!distinctIPs.contains(key)) {
                            long sessionStart = Long.MAX_VALUE;
                            long sessionEnd = Long.MIN_VALUE;

                            for (Tuple2<String, String> element : iterable) {
                                long timestamp = Instant.parse(element.f1).toEpochMilli();
                                sessionStart = Math.min(sessionStart, timestamp);
                                sessionEnd = Math.max(sessionEnd, timestamp);
                            }

                            long sessionDuration = sessionEnd - sessionStart;

                            String result = "IP: " + key + ", Start: " + Instant.ofEpochMilli(sessionStart) +
                                    ", End: " + Instant.ofEpochMilli(sessionEnd) + ", Duration: " + sessionDuration + " milliseconds";

                            collector.collect(result);

                            // Add the IP to the set of distinct IPs
                            distinctIPs.add(key);
                        }
                    }
                }).print();

        // Execute the Flink program
        env.execute("Flink Program for Session Duration Calculation");
    }
}
