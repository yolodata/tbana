package com.yolodata.tbana.cascading.splunk;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.CompositeTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.yolodata.tbana.hadoop.mapred.shuttl.ShuttlCSVInputFormat;
import com.yolodata.tbana.hadoop.mapred.shuttl.ShuttlInputFormatConstants;
import com.yolodata.tbana.hadoop.mapred.util.ArrayListTextWritable;
import com.yolodata.tbana.hadoop.mapred.util.CSVReader;
import com.yolodata.tbana.util.search.ShuttlCsvFileFinder;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.io.InputStreamReader;

public class ShuttlCsv extends TextLine {

    private SplunkDataQuery splunkDataQuery;

    public ShuttlCsv() {
        this(new SplunkDataQuery());
    }

    public ShuttlCsv(SplunkDataQuery splunkDataQuery){
        this(Fields.ALL, splunkDataQuery);
    }

    public ShuttlCsv(Fields fields, SplunkDataQuery splunkDataQuery) {
        super();
        this.splunkDataQuery= splunkDataQuery;
        setSourceFields(fields);
    }

    @Override
    public void sourceConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {

        conf.set(ShuttlInputFormatConstants.INDEX_LIST,splunkDataQuery.getIndexesString());
        conf.set(ShuttlInputFormatConstants.EARLIEST_TIME,splunkDataQuery.getEarliestTime());
        conf.set(ShuttlInputFormatConstants.LATEST_TIME, splunkDataQuery.getLatestTime());
        conf.setInputFormat(ShuttlCSVInputFormat.class);
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
        throw new NotImplementedException("Writing to Shuttl not implemented");
    }

    @Override
    public boolean source(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {
        Tuple result = new Tuple();

        Object key = sourceCall.getContext()[0];
        Object value = sourceCall.getContext()[1];

        boolean hasNext = sourceCall.getInput().next(key, value);
        if (!hasNext) { return false; }

        // Skip nulls
        if (key == null || value == null) { return true; }

        LongWritable keyWritable = (LongWritable) key;
        ArrayListTextWritable values = (ArrayListTextWritable) value;
        result.add(keyWritable);

        for(Text textValue : values)
            result.add(textValue);

        sourceCall.getIncomingEntry().setTuple(result);

        return true;
    }

    @Override
    public Fields retrieveSourceFields( FlowProcess<JobConf> flowProcess, Tap tap )
    {
        // no need to open them all
        if( tap instanceof CompositeTap)
            tap = (Tap) ( (CompositeTap) tap ).getChildTaps().next();


        JobConf conf = flowProcess.getConfigCopy();
        String path = tap.getFullIdentifier(conf);

        try {
            FileSystem fileSystem = FileSystem.get(conf);
            ShuttlCsvFileFinder fileFinder = new ShuttlCsvFileFinder(fileSystem, new Path(path));
            Path pathToOneOfTheBucketCsvs = fileFinder.findSingleFile(splunkDataQuery);

            FSDataInputStream in = fileSystem.open(pathToOneOfTheBucketCsvs);

            CSVReader reader = new CSVReader(new InputStreamReader(in));
            ArrayListTextWritable values = new ArrayListTextWritable();
            if(reader.readLine(values) != 0) {
                // First field is always offset
                Fields fields = new Fields("offset");
                for(Text t : values) {
                    fields = fields.append(new Fields(t.toString()));
                }
                setSourceFields(fields);
            }

        } catch (IOException e) {

        }

        return getSourceFields();
    }


    @Override
    public void sourceCleanup( FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall )
    {
        sourceCall.setContext( null );
    }

    @Override
    public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
        throw new NotImplementedException("Writing to Shuttl not implemented");
    }
}
