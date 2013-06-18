package com.yolodata.tbana.hadoop.mapred.splunk;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.List;

public class ExportInputFormat implements InputFormat<LongWritable, List<Text>> {

    @Override
    public RecordReader<LongWritable, List<Text>> getRecordReader(InputSplit inputSplit,
                                                                  JobConf configuration, Reporter reporter) throws IOException {
        ExportRecordReader splunkExportRecordReader = new ExportRecordReader(configuration);

        splunkExportRecordReader.initialize(inputSplit);

        return splunkExportRecordReader;
    }

    @Override
    public InputSplit[] getSplits(JobConf conf, int numberOfSplits) throws RuntimeException {

        InputSplit[] splits = new InputSplit[numberOfSplits];


        try {
            // ExportRecordReader rr = new ExportRecordReader(conf);
            int resultsPerSplit = 1; //(rr.getNumberOfResults()+1)/numberOfSplits;

            for(int i=0; i<numberOfSplits; i++) {
                int start = i * resultsPerSplit;
                int end = start + resultsPerSplit;
                splits[i] = new SplunkSplit(start, end);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return splits;
    }
}
