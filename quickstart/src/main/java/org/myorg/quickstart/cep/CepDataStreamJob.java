package org.myorg.quickstart.cep;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.myorg.quickstart.cep.condition.ConditionNormal;
import org.myorg.quickstart.cep.condition.ConditionSlow;
import org.myorg.quickstart.cep.model.MetricWatermarks;
import org.myorg.quickstart.cep.model.RowData;
import util.MapUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CepDataStreamJob {


    private static final String brokers = "10.19.90.48:12900,10.19.90.49:12900,10.19.90.50:12900,10.19.90.51:12900";
    private static final String topic = "LOG4X-RUM-TOPIC";
    private static final String groupId = "rum-cep-test";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema()).build();

        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka source");


        DataStream<RowData> stream = kafkaSource
                .flatMap(new RumFlateMap())
                .filter(new FilterFunction<RowData>() {
                    @Override
                    public boolean filter(RowData val) throws Exception {
                        Map<String, Object> fields = val.getFields();
                        String s = MapUtil.deepGetStringValue(fields, "transaction.type", "");
                        return s.equals("user-interaction");
                    }
                })
                .assignTimestampsAndWatermarks(new MetricWatermarks())
                .keyBy(new KeySelector<RowData, String>() {
                    @Override
                    public String getKey(RowData val) throws Exception {
                        Map<String, Object> fields = val.getFields();
                        return MapUtil.deepGetStringValue(fields, "extra.currentPage.id", "");
                    }
                });


        /**
         * 页面卡顿规则
         * 同一个页面 不松散连续3次
         * 点击事务时长 > 3秒
         * 1分钟内
         */
        Pattern<RowData, ?> pattern = Pattern.<RowData>begin("start")
                .where(new ConditionSlow())

                .notFollowedBy("not1")
                .where(new ConditionNormal())
                .followedBy("middle")
                .where(new ConditionSlow())

                .notFollowedBy("not2")
                .where(new ConditionNormal())
                .followedBy("end")
                .where(new ConditionSlow())
                .within(Time.minutes(1));

       /* Pattern<RowData, ?> until = Pattern
                .begin(pattern)
                .oneOrMore()
                .within(Time.minutes(1));*/

        PatternStream<RowData> patternStream = CEP.pattern(stream, pattern);

        SingleOutputStreamOperator<RowData> result = patternStream
                .process(new PatternProcessFunction<RowData, RowData>() {
                    @Override

                    public void processMatch(Map<String, List<RowData>> map, Context context, Collector<RowData> collector) throws Exception {
                        Map<String, Object> data = new HashMap<>();
                        RowData end = map.get("end").get(0);
                        Map<String, Object> fields = end.getFields();

                        data.put("logTime", map.get("logTime"));
                        data.put("browserTime", MapUtil.deepGetLongValue(fields, "transaction.session", 0L));
                        data.put("sessionId", MapUtil.deepGetStringValue(fields, "extra.session.id", ""));
                        data.put("page", MapUtil.deepGetObject(fields, "extra.currentPage"));
                        System.out.println("发现一个卡顿页面");
                        collector.collect(new RowData(data));
                    }
                })
                /*.sinkTo(
                        new Elasticsearch6SinkBuilder<Map>()
                                .setBulkFlushMaxActions(1)
                                .setHosts(
                                        new HttpHost[]{
                                                new HttpHost("10.19.90.48", 12900),
                                                new HttpHost("10.19.90.49", 12900),
                                                new HttpHost("10.19.90.50", 12900),
                                                new HttpHost("10.19.90.51", 12900),
                                        }
                                )
                                .setEmitter((element, context, indexer) -> {
                                    indexer.add(createIndexRequest(element));
                                })
                                .build()
                )*/;
        result.print();
        env.execute("rum-cep-demo");

    }

    private static IndexRequest createIndexRequest(Map element) {
        Map<String, Object> json = new HashMap<>(1);
        json.put("data", element);
        return Requests.indexRequest()
                .index("log4x_rum_cep_0")
                .type("message")
                .source(json);
    }

}
