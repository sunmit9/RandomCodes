/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may
 * not use this file except in compliance with the License. A copy of the
 * License is located at
 *
 *    http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.autodesk.poc.s3.log.parser;

import com.autodesk.poc.s3.log.parser.events.S3.S3LogsSource;
import com.autodesk.poc.s3.log.parser.events.es.ElbStatusCode;
import com.autodesk.poc.s3.log.parser.events.kinesis.Event;
import com.autodesk.poc.s3.log.parser.events.kinesis.LogEvent;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.StreamSupport;

import com.autodesk.poc.s3.log.parser.events.kinesis.PunctuatedAssigner;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.rank.Max;
import org.apache.commons.math3.stat.descriptive.rank.Min;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessLogStream {
    private static final String DEFAULT_REGION = "us-west-2";
    private static final double THRESHOLD = 80;
    private static final OutputTag<Tuple3<String, Integer, Float>> lateOutputTag = new OutputTag<Tuple3<String, Integer, Float>>("late-data") {
    };

    private static final Logger LOG = LoggerFactory.getLogger(ProcessLogStream.class);
    private static final List<String> MANDATORY_PARAMETERS = Arrays.asList("InputStreamName");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Map<String, Properties> applicationProperties;

        if (env instanceof LocalStreamEnvironment) {
            applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties(ProcessLogStream.class.getClassLoader().getResource("testProperties.json").getPath());
        } else {
            applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        }

        if (applicationProperties == null) {
            LOG.error("Unable to load application properties from the Kinesis Analytics Runtime. Exiting.");

            return;
        }

        Properties flinkProperties = applicationProperties.get("FlinkApplicationProperties");

        if (flinkProperties == null) {
            LOG.error("Unable to load FlinkApplicationProperties properties from the Kinesis Analytics Runtime. Exiting.");

            return;
        }

        if (!flinkProperties.keySet().containsAll(MANDATORY_PARAMETERS)) {
            LOG.error("Missing mandatory parameters. Expected '{}' but found '{}'. Exiting.",
                    String.join(", ", MANDATORY_PARAMETERS),
                    flinkProperties.keySet());

            return;
        }
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        if (flinkProperties.getProperty("EventTime", "true").equals("true")) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        }


        /*DataStream<LogEvent> logsFromS3Sn = env.addSource(new S3LogsSource())
            .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LogEvent>() {
                // This will assign the event timestamp to the stream as default timestamp for windowing
                // And sorts the events in ascending order before the events are processed
                @Override
                public long extractAscendingTimestamp(LogEvent logEvent) {
                    return logEvent.getTimestamp();
                }
            });*/


        DataStream<LogEvent> logsFromS3Mks = env.addSource(new S3LogsSource())
                .assignTimestampsAndWatermarks(new PunctuatedAssigner())
                .filter(event -> LogEvent.class.isAssignableFrom(event.getClass()))
                .map(event -> (LogEvent) event);
               // .setParallelism(2);

        SingleOutputStreamOperator<ElbStatusCode> elbStatusCount = logsFromS3Mks
            .map(new MapFunction<LogEvent, Tuple3<String, Integer, Float>>() {
                @Override
                public Tuple3<String, Integer, Float> map(LogEvent logData) throws Exception {
                    return new Tuple3<String, Integer, Float>(logData.getMoniker(),
                            logData.getElb_status_code(), logData.getRequest_processing_time()
                            + logData.getTarget_processing_time() + logData.getResponse_processing_time());
                }
            })

            .keyBy(0)
                // keyBy is grouping the aggregations by the keys specified
                // 0 -> moniker

            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                // Creates a new tumbling window for each aggregation.
                // This window is based on the event timestamp we assign above

            .allowedLateness(Time.seconds(0))
                // Wait for 10 minutes, and before calling the apply function depending on the duration of tumbling window
                // Waiting allows to take the late events into consideration.
                // This does not affect the tumbling window duration
            .sideOutputLateData(lateOutputTag)
            .process(new ProcessWindowFunction<Tuple3<String, Integer, Float>, ElbStatusCode, Tuple, TimeWindow>() {
                @Override
                public void process(Tuple key, Context context, Iterable<Tuple3<String, Integer, Float>> input, Collector<ElbStatusCode> collector) throws Exception {
                    final ElbStatusCode elbdata = new ElbStatusCode();
                    int i = 0;
                    int count500 = 0;
                    int reqCount = Iterables.size(input);
                    double[] elbTotalTime = new double[reqCount];
                    int reqGrtThanThreshold = 0;
                    final Percentile p = new Percentile();

                    for(Tuple3<String, Integer, Float> t : input){
                        elbTotalTime[i] = (double) t.f2;
                        if (t.f1 >= 500) {
                            count500++;
                        }
                        if (elbTotalTime[i] > THRESHOLD){
                            reqGrtThanThreshold++;
                        }
                    }
                    elbdata.setWindowStart(context.window().getStart());
                    elbdata.setWindowStop(context.window().getEnd());

                    elbdata.setElbStatusCode(Iterables.get(input, 0).f1);
                    elbdata.setMoniker(Iterables.get(input, 0).f0);
                    //elbdata.setP50(p.evaluate(elbTotalTime, 50));
                    //elbdata.setP90(p.evaluate(elbTotalTime, 90));
                    elbdata.setP95(p.evaluate(elbTotalTime, 95));
                    elbdata.setP99(p.evaluate(elbTotalTime, 99));
                    elbdata.setAverage(new Mean().evaluate(elbTotalTime));
                    elbdata.setMin(new Min().evaluate(elbTotalTime));
                    elbdata.setMax(new Max().evaluate(elbTotalTime));
                    elbdata.setCount(Iterables.size(input));
                    elbdata.setCount(reqCount);
                    elbdata.setCount500(count500);
                    elbdata.setPercentage500((double)count500/(double)reqCount);
                    elbdata.setRequestGreaterThanThreshold(reqGrtThanThreshold);
                    //System.out.print(elbdata.toString());
                    collector.collect(elbdata);
                }
            });

        DataStream<Tuple3<String, Integer, Float>> lateData = elbStatusCount.getSideOutput(lateOutputTag);

        if (flinkProperties.containsKey("ElasticsearchEndpoint")) {
            final String elasticsearchEndpoint = flinkProperties.getProperty("ElasticsearchEndpoint");
            final String region = flinkProperties.getProperty("Region", DEFAULT_REGION);

            //elbStatusCount.addSink(null);
            // elbStatusCount.addSink(AmazonElasticsearchSink.buildElasticsearchSink(elasticsearchEndpoint, region, "pickup_count", "pickup_count"));
        }

        elbStatusCount.print().setParallelism(1);
        lateData.print();
        LOG.info("Starting to consume events from stream {}", flinkProperties.getProperty("InputStreamName"));

        env.execute();
    }

}
