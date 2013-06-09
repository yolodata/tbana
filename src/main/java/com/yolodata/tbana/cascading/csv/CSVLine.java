package com.yolodata.tbana.cascading.csv;


import cascading.flow.FlowProcess;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import com.yolodata.tbana.hadoop.mapred.CSVNLineInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import java.util.Arrays;

public class CSVLine extends TextLine {

    @Override
    public void sourceConfInit( FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf )
    {
        if( hasZippedFiles(FileInputFormat.getInputPaths(conf)) )
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
}
