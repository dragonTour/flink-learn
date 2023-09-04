package org.myorg.quickstart.cep.condition;

import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.myorg.quickstart.cep.model.RowData;
import util.MapUtil;

import java.util.Map;

public class ConditionNormal extends SimpleCondition<RowData> {
    @Override
    public boolean filter(RowData data) throws Exception {
        Map<String, Object> fields = data.getFields();
        Long aLong = MapUtil.deepGetLongValue(fields, "transaction.duration", 0L);
        return aLong <= 200;
    }
}
