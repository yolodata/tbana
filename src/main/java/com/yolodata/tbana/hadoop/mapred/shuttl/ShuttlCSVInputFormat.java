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
        for(FileStatus status : listStatus(job)) {
            List<FileSplit> fileSplits = getSplitsForFile(status, job, numSplits);
            splits.addAll(fileSplits);
        }

        return splits.toArray(new InputSplit[splits.size()]);
    }

    protected static List<FileSplit> getSplitsForFile(FileStatus status, JobConf conf, int numSplits)
            throws IOException {
        Path filePath = getPath(status);
        FileSystem fs = filePath.getFileSystem(conf);
        if(numSplits==0)
            numSplits++;
        return getFileSplitsFast(numSplits, filePath, fs);
    }

    private static List<FileSplit> getFileSplitsFast(int numSplits, Path filePath, FileSystem fs) {
        List<FileSplit> splits = new ArrayList<FileSplit>();

        try {
            long fileSize = fs.getFileStatus(filePath).getLen();
            long sizePerSplit = fileSize/(numSplits+1);

            long start=0,end=0;

            while(end<fileSize) {
                end = start+sizePerSplit;

                if(restOfFileChunkFitsInOneSplit(fileSize, end)) {
                    splits.add(createSplit(filePath,start,fileSize));
                    break;
                }

                // Seek to current end and find the new line
                FSDataInputStream in = fs.open(filePath);
                end = findEndOfLinePosition(in,end);

                splits.add(createSplit(filePath,start,end-start));
                start=end;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return splits;
    }

    private static boolean restOfFileChunkFitsInOneSplit(long fileSize, long end) {
        return end >= fileSize;
    }

    // TODO: This is a heuristic that might not be true in all cases
    private static long findEndOfLinePosition(FSDataInputStream in, long end) throws IOException {
        in.seek(end);
        int c;
        String findNewLineBuffer = "";
        while((c=in.read())!= -1) {
            char ch = (char)c;

            // a real new line is found!
            if(findNewLineBuffer == "\"\n" && ch != ',')
                return in.getPos();

            if(findNewLineBuffer == "\"" && ch == '\n') {
                findNewLineBuffer = findNewLineBuffer.concat("\n");
                continue;
            }

            if(ch == '\"' && findNewLineBuffer.length()==0) {
                findNewLineBuffer = "\"";
                continue;
            }
            findNewLineBuffer = "";

        }
        return in.getPos();
    }

    private static FileSplit createSplit(Path filePath, long start, long end) {
        return new FileSplit(filePath,start,end, new String[] {});
    }

    private static Path getPath(FileStatus status) throws IOException {
        Path fileName = status.getPath();
        if (status.isDir()) {
            throw new IOException("Not a file: " + fileName);
        }
        return fileName;
    }

}
