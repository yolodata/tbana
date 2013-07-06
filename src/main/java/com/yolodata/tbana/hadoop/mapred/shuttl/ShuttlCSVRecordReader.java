package com.yolodata.tbana.hadoop.mapred.shuttl;

import com.yolodata.tbana.hadoop.mapred.util.ArrayListTextWritable;
import com.yolodata.tbana.hadoop.mapred.util.CSVReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import java.io.*;
import java.util.List;

public class ShuttlCSVRecordReader implements RecordReader<LongWritable, List<Text>> {

    protected InputStream is;
    protected CSVReader reader;

    private long start;
    private long pos;
    private long end;

    private long startKey;

    public ShuttlCSVRecordReader() {

    }

    public ShuttlCSVRecordReader(InputStream is, JobConf conf) throws IOException {
        this.is = is;
        createReader(is);
    }

    public void createReader(InputStream is) throws IOException {
        this.reader = new CSVReader(new BufferedReader(new InputStreamReader(is)));
    }

    public void initialize(InputSplit genericSplit, JobConf conf) throws IOException {
        CsvSplit split = (CsvSplit) genericSplit;

        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getFilepath();

        startKey = split.getKeyStart();
        FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream fileIn = fs.open(split.getFilepath());

        if (start != 0) {
            fileIn.seek(start);
        }
        this.is = fileIn;
        this.pos = start;

        createReader(is);

        if(split.isSkipHeader())
            next(null,null);
    }

    @Override
    public float getProgress() {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    public synchronized void close() throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
        if (is != null) {
            is.close();
            is = null;
        }
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public List<Text> createValue() {
        return new ArrayListTextWritable();
    }

    @Override
    public long getPos() throws IOException {
        return pos;
    }

    @Override
    public boolean next(LongWritable key, List<Text> value) throws IOException {
        if(pos==end) {
            return false;
        }
        if (key == null) {
            key = new LongWritable();
        }
        key.set(startKey+pos);

        if (value == null) {
            value = new ArrayListTextWritable();
        }

        int bytesRead = reader.readLine(value);

        pos += bytesRead;
        if (bytesRead == 0) {
            key = null;
            value = null;
            return false;
        }

        removeNewLineOnLastColumn(value);
        return true;

    }

    private void removeNewLineOnLastColumn(List<Text> value) {
        String lastColumn = value.get(value.size()-1).toString();
        if(lastColumn.endsWith("\n"))
            value.set(value.size()-1, new Text(lastColumn.substring(0,lastColumn.length()-1)));
    }
}
