package org.myorg.quickstart.learn.tableApi.model;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.myorg.quickstart.cep.model.RowData;
import util.MapUtil;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Map;

public class MetricWatermarks implements AssignerWithPeriodicWatermarks<RumModel> {
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
    public long extractTimestamp(RumModel data, long l) {
        long timeStamp = data.getLogTime();
        currentTimeStamp = Math.max(timeStamp, currentTimeStamp);
        return timeStamp;
    }
}
