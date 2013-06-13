package com.yolodata.tbana.hadoop.mapred.splunk;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.List;

import com.splunk.*;

public class SplunkInputFormat implements InputFormat<LongWritable, List<Text>> {

    @Override
    public RecordReader<LongWritable, List<Text>> getRecordReader(InputSplit inputSplit,
                                                                  JobConf configuration, Reporter reporter) throws IOException {
        SplunkExportRecordReader splunkExportRecordReader = new SplunkExportRecordReader(configuration);

        splunkExportRecordReader.initialize(inputSplit);

        return splunkExportRecordReader;
    }

    @Override
    public InputSplit[] getSplits(JobConf conf, int numberOfSplits) throws IOException {

        InputSplit[] splits = new InputSplit[numberOfSplits];

        SplunkExportRecordReader rr = new SplunkExportRecordReader(conf);

        int resultsPerSplit = rr.getNumberOfResults()/numberOfSplits;

        for(int i=0; i<numberOfSplits; i++) {
            int start = i * resultsPerSplit;
            int end = start + resultsPerSplit;
            splits[i] = new SplunkSplit(start, end);
        }

        return splits;
    }
}
