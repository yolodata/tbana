package com.yolodata.tbana.cascading.splunk;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import com.yolodata.tbana.hadoop.mapred.shuttl.ShuttlCSVInputFormat;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

public class ShuttlCsv extends TextLine {

    @Override
    public void sourceConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {



        conf.setInputFormat( ShuttlCSVInputFormat.class );
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
        throw new NotImplementedException("Writing to Shuttl not implemented");
    }

    @Override
    public boolean source(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {
        if( !sourceReadInput( sourceCall ) )
            return false;

        sourceHandleInput( sourceCall );

        return true;
    }

    private boolean sourceReadInput( SourceCall<Object[], RecordReader> sourceCall ) throws IOException
    {
        Object[] context = sourceCall.getContext();
        return sourceCall.getInput().next( context[ 0 ], context[ 1 ] );
    }

    protected void sourceHandleInput( SourceCall<Object[], RecordReader> sourceCall )
    {
        Tuple tuple = sourceCall.getIncomingEntry().getTuple();

        int index = 0;

        Object[] context = sourceCall.getContext();

        if( getSourceFields().size() == 2 )
            tuple.set( index++, ( (LongWritable) context[ 0 ] ).get() );

        tuple.set( index, context[1] );
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
