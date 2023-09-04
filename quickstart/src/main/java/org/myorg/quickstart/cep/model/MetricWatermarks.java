package org.myorg.quickstart.cep.model;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import util.MapUtil;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

public class MetricWatermarks implements AssignerWithPeriodicWatermarks<RowData> {
    private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    long currentTimeStamp = 0L;
    final long maxDelayAllowed = 10L;
    long currentWaterMark;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        currentWaterMark = currentTimeStamp - maxDelayAllowed;
        return new Watermark(currentWaterMark);
    }

    @Override
    public long extractTimestamp(RowData data, long l) {
        long timeStamp = 0;
        Map<String, Object> fields = data.getFields();
        timeStamp = MapUtil.deepGetLongValue(fields, "logTime", 0L);
        currentTimeStamp = Math.max(timeStamp, currentTimeStamp);
        return timeStamp;
    }
}
