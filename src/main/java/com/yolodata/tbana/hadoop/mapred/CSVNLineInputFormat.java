package com.yolodata.tbana.hadoop.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class CSVNLineInputFormat extends FileInputFormat<LongWritable, List<Text>> {

	public static final String LINES_PER_MAP = "mapreduce.input.lineinputformat.linespermap";

	public static final int DEFAULT_LINES_PER_MAP = 1;

	@Override
	public RecordReader<LongWritable, List<Text>> getRecordReader(
			InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {

		String quote = jobConf.get(CSVLineRecordReader.FORMAT_DELIMITER, CSVLineRecordReader.DEFAULT_DELIMITER);
		String separator = jobConf.get(CSVLineRecordReader.FORMAT_SEPARATOR, CSVLineRecordReader.DEFAULT_SEPARATOR);
		
		if(quote == null || separator == null) {
			throw new IOException("CSVTextInputFormat: missing parameter delimiter");
		}
		reporter.setStatus(inputSplit.toString());
			
		CSVLineRecordReader reader = new CSVLineRecordReader();
		reader.initialize(inputSplit, jobConf);

		return reader;
	}

	public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
		List<InputSplit> splits = new ArrayList<InputSplit>();
		int numLinesPerSplit = getNumLinesPerSplit(job);
		for(FileStatus status : listStatus(job)) {
			List<FileSplit> fileSplits = getSplitsForFile(status, job, numLinesPerSplit);
			splits.addAll(fileSplits);
		}
		
		return splits.toArray(new InputSplit[splits.size()]);
	}

	private static List<FileSplit> getSplitsForFile(FileStatus status, JobConf conf, int numLinesPerSplit)
			throws IOException {
		List<FileSplit> splits = new ArrayList<FileSplit>();
		Path fileName = status.getPath();
		if (status.isDir()) {
			throw new IOException("Not a file: " + fileName);
		}
		FileSystem fs = fileName.getFileSystem(conf);
		CSVLineRecordReader lr = null;
		try {
			FSDataInputStream in = fs.open(fileName);
			lr = new CSVLineRecordReader(in, conf);
			List<Text> line = new ArrayList<Text>();
			int numLines = 0;
			long begin = 0;
			long length = 0;
			int num = -1;
			while ((num = lr.readLine(line)) > 0) {
				numLines++;
				length += num;
				if (numLines == numLinesPerSplit) {
					// To make sure that each mapper gets N lines,
					// we move back the upper split limits of each split
					// by one character here.
					if (begin == 0) {
						splits.add(new FileSplit(fileName, begin, length - 1, new String[] {}));
					} else {
						splits.add(new FileSplit(fileName, begin, length - 1, new String[] {}));
					}
					begin += length;
					length = 0;
					numLines = 0;
				}
			}
			if (numLines != 0) {
				splits.add(new FileSplit(fileName, begin, length, new String[] {}));
			}
		} finally {
			if (lr != null) {
				lr.close();
			}
		}
		return splits;
	}

	public static int getNumLinesPerSplit(JobConf job) {
		return job.getInt(LINES_PER_MAP, DEFAULT_LINES_PER_MAP);
	}

}
