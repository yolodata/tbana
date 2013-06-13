package com.yolodata.tbana.hadoop.mapred.splunk;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.List;

import com.splunk.*;

public class SplunkInputFormat extends FileInputFormat<LongWritable, List<Text>> {

    @Override
    public RecordReader<LongWritable, List<Text>> getRecordReader(InputSplit inputSplit,
                                                                  JobConf entries, Reporter reporter) throws IOException {

        //TODO: Check JobConf for user, password, address:port, searchMode(realtime/normal)


        return null;
    }
}
