package com.yolodata.tbana.hadoop.mapred;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class CSVTextInputFormat extends FileInputFormat<LongWritable, List<Text>>{

	@Override
	public RecordReader<LongWritable, List<Text>> getRecordReader(
			InputSplit split, JobConf jobConf, Reporter reporter) throws IOException {
        if(jobConf.get(CSVLineRecordReader.FORMAT_DELIMITER) == null ||
				jobConf.get(CSVLineRecordReader.FORMAT_SEPARATOR) == null) {
			throw new IOException("CSVTextInputFormat: missing parameter delimiter/separator");
		}
		
		CSVLineRecordReader reader = new CSVLineRecordReader();
        reader.initialize(split, jobConf);
		return reader;
		
	}
}
