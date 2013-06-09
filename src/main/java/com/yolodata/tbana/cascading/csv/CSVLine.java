package com.yolodata.tbana.cascading.csv;


import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import com.yolodata.tbana.hadoop.mapred.CSVNLineInputFormat;
import com.yolodata.tbana.hadoop.mapred.CSVTextInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

public class CSVLine extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {

    public enum Compress {
        DISABLE,
        ENABLE
    }

    public Compress getSinkCompression() {
        return Compress.DISABLE;
    }

    /** Field DEFAULT_SOURCE_FIELDS */
    public static final Fields DEFAULT_SOURCE_FIELDS = new Fields( "offset", "columns" );
    private String charsetName = "UTF-8";

    public CSVLine()
    {
        super( DEFAULT_SOURCE_FIELDS );
    }

    @Override
    public void sourceConfInit( FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf )
    {
        if( hasZippedFiles( FileInputFormat.getInputPaths(conf) ) )
            throw new IllegalStateException( "cannot read zip files: " + Arrays.toString(FileInputFormat.getInputPaths(conf)) );

        conf.setInputFormat( CSVNLineInputFormat.class );
    }

    private boolean hasZippedFiles( Path[] paths )
    {
        boolean isZipped = paths[ 0 ].getName().endsWith( ".zip" );

        for( int i = 1; i < paths.length; i++ )
        {
            if( isZipped != paths[ i ].getName().endsWith( ".zip" ) )
                throw new IllegalStateException( "cannot mix zipped and upzipped files" );
        }

        return isZipped;
    }

    @Override
    public void presentSourceFields( FlowProcess<JobConf> flowProcess, Tap tap, Fields fields )
    {
        // do nothing to change CSVLine state
    }

    @Override
    public void presentSinkFields( FlowProcess<JobConf> flowProcess, Tap tap, Fields fields )
    {
        // do nothing to change CSVLine state
    }

    @Override
    public void sinkConfInit( FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf )
    {
        if( tap.getFullIdentifier( conf ).endsWith( ".zip" ) )
            throw new IllegalStateException( "cannot write zip files: " + FileOutputFormat.getOutputPath( conf ) );

        if( getSinkCompression() == Compress.DISABLE )
            conf.setBoolean( "mapred.output.compress", false );
        else if( getSinkCompression() == Compress.ENABLE )
            conf.setBoolean( "mapred.output.compress", true );

        conf.setOutputKeyClass( Text.class ); // be explicit
        conf.setOutputValueClass( Text.class ); // be explicit
        conf.setOutputFormat( TextOutputFormat.class );
    }

    @Override
    public void sourcePrepare( FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall )
    {
        if( sourceCall.getContext() == null )
            sourceCall.setContext( new Object[ 3 ] );

        sourceCall.getContext()[ 0 ] = sourceCall.getInput().createKey();
        sourceCall.getContext()[ 1 ] = sourceCall.getInput().createValue();
        sourceCall.getContext()[ 2 ] = Charset.forName(charsetName);
    }

    @Override
    public boolean source( FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall ) throws IOException
    {
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
        TupleEntry result = sourceCall.getIncomingEntry();

        int index = 0;
        Object[] context = sourceCall.getContext();

        // coerce into canonical forms
        if( getSourceFields().size() == 2 )
            result.setLong( index++, ( (LongWritable) context[ 0 ] ).get() );

        result.setString(index, makeEncodedString(context));
    }

    protected String makeEncodedString( Object[] context )
    {
        Text text = (Text) context[ 1 ];
        return new String( text.getBytes(), 0, text.getLength(), (Charset) context[ 2 ] );
    }

    @Override
    public void sourceCleanup( FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall )
    {
        sourceCall.setContext( null );
    }

    @Override
    public void sinkPrepare( FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall ) throws IOException
    {
        sinkCall.setContext( new Object[ 2 ] );

        sinkCall.getContext()[ 0 ] = new Text();
        sinkCall.getContext()[ 1 ] = Charset.forName( charsetName );
    }

    @Override
    public void sink( FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall ) throws IOException
    {
        Text text = (Text) sinkCall.getContext()[ 0 ];
        Charset charset = (Charset) sinkCall.getContext()[ 1 ];
        String line = sinkCall.getOutgoingEntry().getTuple().toString();

        text.set( line.getBytes( charset ) );

        // it's ok to use NULL here so the collector does not write anything
        sinkCall.getOutput().collect( null, text );
    }
}
