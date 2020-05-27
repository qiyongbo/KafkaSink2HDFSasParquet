package com.utils;

import org.apache.avro.generic.GenericData;
import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.bucketing.BasePathBucketer;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DayHourPathBucketer extends BasePathBucketer<GenericData.Record> {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd"+File.separator+"HH");
    @Override
    public Path getBucketPath(Clock clock, Path basePath, GenericData.Record element) {
        String data = (String) element.get("data");
        long event_time = Long.parseLong(data.split("\t")[0]);
        String day_hour = sdf.format(new Date(event_time));
        return  new Path(basePath+File.separator+day_hour);
    }
}