/*
 * Copyright (c) 2013 Yolodata, LLC,  All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yolodata.tbana.hadoop.mapred.csv;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

	protected static List<FileSplit> getSplitsForFile(FileStatus status, JobConf conf, int numLinesPerSplit)
			throws IOException {
        Path filePath = getPath(status);
		FileSystem fs = filePath.getFileSystem(conf);
        return getFileSplits(conf, numLinesPerSplit, filePath, fs);
	}

    private static List<FileSplit> getFileSplits(JobConf conf, int numLinesPerSplit, Path filePath, FileSystem fs) throws IOException {

        List<FileSplit> splits = new ArrayList<FileSplit>();

        CSVLineRecordReader lr = null;
        try {
            FSDataInputStream in = fs.open(filePath);
            lr = new CSVLineRecordReader(in, conf);
            List<Text> line = new ArrayList<Text>();
            int numLines = 0;
            long startPos = 0;
            long splitLength = 0;
            int num;
            while ((num = lr.readLine(line)) > 0) {
                numLines++;
                splitLength += num;
                if (numLines == numLinesPerSplit) {
                    splits.add(new FileSplit(filePath, startPos, splitLength - 1, new String[] {}));
                    startPos += splitLength;
                    splitLength = 0;
                    numLines = 0;
                }
            }
            if (numLines != 0) {
                splits.add(new FileSplit(filePath, startPos, splitLength, new String[] {}));
            }
        } finally {
            if (lr != null) lr.close();
        }
        return splits;
    }

    private static Path getPath(FileStatus status) throws IOException {
        Path fileName = status.getPath();
        if (status.isDir()) {
            throw new IOException("Not a file: " + fileName);
        }
        return fileName;
    }

    public static int getNumLinesPerSplit(JobConf job) {
		return job.getInt(LINES_PER_MAP, DEFAULT_LINES_PER_MAP);
	}

}
