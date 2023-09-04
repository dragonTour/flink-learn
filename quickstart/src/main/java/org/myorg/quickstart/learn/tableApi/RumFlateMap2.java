package org.myorg.quickstart.learn.tableApi;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.shaded.curator5.com.google.common.collect.Maps;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.cep.model.RowData;
import org.myorg.quickstart.learn.tableApi.model.RumModel;
import util.MapUtil;

import java.util.*;


public class RumFlateMap2 extends RichFlatMapFunction<String, RumModel> {


    @Override
    public void flatMap(String s, Collector<RumModel> collector) throws Exception {

        s = "[" + s.replace("\n", ",") + "]";


        ObjectMapper objectMapper = new ObjectMapper();
        List<Map<String, Object>> l = objectMapper.readValue(s, new TypeReference<List<Map<String, Object>>>() {
        });

        List<Map> transactionlist = new ArrayList<>();
        List<Map> spanlist = new ArrayList<>();
        List<Map> errorlist = new ArrayList<>();

        Map metadata = new HashMap();
        Map extra = new HashMap();


        Long logTime = null;

        for (Map<String, Object> m : l) {
            Set<Map.Entry<String, Object>> entries = m.entrySet();
            for (Map.Entry<String, Object> entry : entries) {
                if (entry.getKey().equals("metadata")) {
                    metadata = (Map) entry.getValue();
                } else if (entry.getKey().equals("extra")) {
                    extra = (Map) entry.getValue();
                }else if (entry.getKey().equals("span")){
                    spanlist.add((Map) entry.getValue());
                }else if(entry.getKey().equals("transaction")){
                    transactionlist.add((Map) entry.getValue());
                }else if (entry.getKey().equals("error")){
                    errorlist.add((Map) entry.getValue());
                }else if (entry.getKey().equals("logTime")){
                    logTime = (Long) entry.getValue();
                }
            }
        }

        for (Map map : transactionlist) {
            RumModel rumModel = new RumModel();
            rumModel.setTransactionType(MapUtil.deepGetStringValue(map, "type", ""));
            rumModel.setSessionId(MapUtil.deepGetStringValue(extra, "session.id", ""));
            rumModel.setPageId(MapUtil.deepGetStringValue(extra, "currentPage.id", ""));
//            rumModel.setPageUrl(MapUtil.deepGetStringValue(extra, "currentPage.url", ""));
            rumModel.setDuration(MapUtil.deepGetLongValue(map, "duration", 0L));
            rumModel.setLogTime(logTime);
            collector.collect(rumModel);
        }
    }
}

