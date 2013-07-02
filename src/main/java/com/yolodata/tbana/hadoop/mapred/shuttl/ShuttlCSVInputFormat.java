package com.yolodata.tbana.hadoop.mapred.shuttl;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.yolodata.tbana.hadoop.mapred.util.CSVReader;
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

public class ShuttlCSVInputFormat extends FileInputFormat<LongWritable, List<Text>> {

    @Override
    public RecordReader<LongWritable, List<Text>> getRecordReader(
            InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        reporter.setStatus(inputSplit.toString());

        ShuttlCSVRecordReader reader = new ShuttlCSVRecordReader();
        reader.initialize(inputSplit, jobConf);

        return reader;
    }

    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        int numLinesPerSplit = 0;
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

        CSVReader lr = null;
        try {
            FSDataInputStream in = fs.open(filePath);
            lr = new CSVReader(new InputStreamReader(in));
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

}
