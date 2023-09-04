package org.myorg.quickstart.learn.tableApi;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.types.Row;
import org.myorg.quickstart.learn.tableApi.model.MetricWatermarks;
import org.myorg.quickstart.learn.tableApi.model.RumModel;

import java.time.Duration;
import java.util.Set;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class Main {

    private static final String brokers = "10.19.90.48:12900,10.19.90.49:12900,10.19.90.50:12900,10.19.90.51:12900";
    private static final String topic = "LOG4X-RUM-TOPIC";
    private static final String groupId = "rum-cep-test";


    public static void main(String[] args) throws Exception {
        /*TableEnvironment tableEnv = TableEnvironment.create(*//*…*//*);

// Create a source table
        tableEnv.createTemporaryTable("SourceTable", TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .build())
                .option(DataGenConnectorOptions.ROWS_PER_SECOND, 100L)
                .build());

// Create a sink table (using SQL DDL)
        tableEnv.executeSql("CREATE TEMPORARY TABLE SinkTable WITH ('connector' = 'blackhole') LIKE SourceTable (EXCLUDING OPTIONS) ");

// Create a Table object from a Table API query
        Table table1 = tableEnv.from("SourceTable");

// Create a Table object from a SQL query
        Table table2 = tableEnv.sqlQuery("SELECT * FROM SourceTable");

// Emit a Table API result Table to a TableSink, same for SQL result
        TableResult tableResult = table1.insertInto("SinkTable").execute();*/

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema()).build();

        SingleOutputStreamOperator<RumModel> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka source")
                .flatMap(new RumFlateMap2())
                .assignTimestampsAndWatermarks(
                        new MetricWatermarks()
//                        WatermarkStrategy
//                                .<RumModel>forBoundedOutOfOrderness(Duration.ofSeconds(2))
//                                .withTimestampAssigner((event, timestamp) -> event.getLogTime())
                );

//        Table inputTable = tableEnv.fromDataStream(kafkaSource);
        Table inputTable = tableEnv.fromDataStream(kafkaSource,
                $("transactionType"),
                $("sessionId"),
                $("pageId"),
                $("logTime"),
                $("duration"),
                $("eventTime").rowtime() //定义时间时间字段
        );
//        tableEnv.createTemporaryView("rumTable", kafkaSource);
//        tableEnv.createTemporaryView("rumTable", inputTable);

//        Table resultTable = tableEnv.from("rumTable")
//        Table resultTable = inputTable
//                .window(Tumble.over(lit(10).seconds()).on($("eventTime")).as("w")) //滚动窗口
////                .groupBy($("w"),$("sessionId"))
//                .groupBy($("w"))
////                .select($("transactionType"), $("sessionId"), $("pageId"),
////                        $("duration"),
////                        $("eventTime")
////                )
////                .where($("transactionType").isEqual("user-interaction"))
//                .select($("sessionId").count());


//        tableEnv.createTemporaryView("rumTable", kafkaSource);
//        tableEnv.createTemporaryView("rumTable", resultTable);
        tableEnv.createTemporaryView("rumTable", inputTable);
//        Table resultTable2 = tableEnv.sqlQuery("SELECT * FROM rumTable");
//        Table resultTable = tableEnv.sqlQuery("SELECT count(sessionId) as sessionId, window_start, window_end FROM TABLE ( TUMBLE(TABLE rumTable, DESCRIPTOR(eventTime),INTERVAL '10' SECONDS)) group by window_start, window_end");
        Table resultTable = tableEnv.sqlQuery("SELECT pageId,count(pageId) FROM rumTable group by pageId,TUMBLE(eventTime,INTERVAL '10' SECONDS)");


        tableEnv.toDataStream(resultTable).print();
/*        tableEnv.executeSql("CREATE TABLE fs_table (" +
                "  sessionId BIGINT ," +
                "  window_start TIMESTAMP(3), " +
                "  window_end TIMESTAMP(3) " +
                ") WITH (" +
                "  'connector'='filesystem'," +
                "  'path'='/Users/zhaofeilong/job/learn/flink/quickstart/print'," +
                "  'format'='csv'" +
                ")");
//        resultTable.executeInsert("fs_table");
        resultTable.insertInto("fs_table").execute();*/
   /*     resultStream.addSink(new SinkFunction<Row>() {
            @Override
            public void invoke(Row value) throws Exception {
                Set<String> fieldNames = value.getFieldNames(true);
                for (String fieldName : fieldNames) {
                    System.out.print(fieldName + " : " + value.getField(fieldName) + " | ");
                }
                System.out.println();
            }
        });*/

        env.execute();

    }
}
