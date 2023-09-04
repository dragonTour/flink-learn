package org.myorg.quickstart.cep;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.shaded.curator5.com.google.common.collect.Maps;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.cep.model.RowData;

import java.util.*;


public class RumFlateMap extends RichFlatMapFunction<String, RowData> {


    @Override
    public void flatMap(String s, Collector<RowData> collector) throws Exception {

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
            Map data = new HashMap();

            data.put("metadata",metadata);
            data.put("extra",extra);
            Map<String,String> processor = Maps.newHashMap();
            processor.put("event","transaction");
            data.put("processor",processor);
            data.put("logTime",logTime);
            data.put("transaction",map);
            collector.collect(new RowData(data));
        }
/*        for (Map map : spanlist) {
            map.put("metadata",metadata);
            map.put("extra",extra);
            Map<String,String> processor = Maps.newHashMap();
            processor.put("event","span");
            map.put("processor",processor);
            map.put("logTime",logTime);
            collector.collect(map);
        }

        for (Map map : errorlist) {
            map.put("metadata",metadata);
            map.put("extra",extra);
            Map<String,String> processor = Maps.newHashMap();
            processor.put("event","error");
            map.put("processor",processor);
            map.put("logTime",logTime);
            collector.collect(map);
        }*/
    }
}

