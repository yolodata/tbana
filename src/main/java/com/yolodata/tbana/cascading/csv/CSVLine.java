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

package com.yolodata.tbana.cascading.csv;


import cascading.flow.FlowProcess;
import cascading.scheme.SourceCall;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import com.yolodata.tbana.hadoop.mapred.csv.CSVLineRecordReader;
import com.yolodata.tbana.hadoop.mapred.csv.CSVNLineInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.util.Arrays;

public class CSVLine extends TextLine {

    @Override
    public void sourceConfInit( FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf )
    {
        if( hasZippedFiles(FileInputFormat.getInputPaths(conf)) )
            throw new IllegalStateException( "cannot read zip files: " + Arrays.toString(FileInputFormat.getInputPaths(conf)) );

        conf.set(CSVLineRecordReader.FORMAT_DELIMITER,CSVLineRecordReader.DEFAULT_DELIMITER);
        conf.set(CSVLineRecordReader.FORMAT_SEPARATOR,CSVLineRecordReader.DEFAULT_SEPARATOR);
        conf.setBoolean(CSVLineRecordReader.IS_ZIPFILE, false);
        conf.setInt(CSVNLineInputFormat.LINES_PER_MAP, 40000);

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
        Tuple tuple = sourceCall.getIncomingEntry().getTuple();

        int index = 0;

        Object[] context = sourceCall.getContext();

        if( getSourceFields().size() == 2 )
            tuple.set( index++, ( (LongWritable) context[ 0 ] ).get() );

        tuple.set( index, context[1] );

    }

}
