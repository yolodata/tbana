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

package com.yolodata.tbana.cascading.splunk;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkConf;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkDataQuery;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkInputFormat;
import com.yolodata.tbana.hadoop.mapred.util.ArrayListTextWritable;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.io.Serializable;

public class SplunkScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> implements Serializable {

    private SplunkDataQuery splunkDataQuery;

    public SplunkScheme(SplunkDataQuery splunkDataQuery) {
        this.splunkDataQuery = splunkDataQuery;
        setFields(Fields.ALL);
    }

    private void setFields(Fields fields) {
        if(!fields.isUnknown() && !fields.isAll())
            return;

        if(!fields.contains(new Fields("offset"))) {
            fields = new Fields("offset").append(fields);
        }

        setSourceFields(fields);
    }

    @Override
    public void sourceConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {

        conf.setInputFormat(SplunkInputFormat.class);
        conf.set(SplunkConf.SPLUNK_SEARCH_QUERY, this.splunkDataQuery.getSplunkQuery());
        conf.set(SplunkConf.SPLUNK_EARLIEST_TIME, this.splunkDataQuery.getEarliestTimeString());
        conf.set(SplunkConf.SPLUNK_LATEST_TIME, this.splunkDataQuery.getLatestTimeString());
    }

    @Override
    public void sourcePrepare(FlowProcess<JobConf> flowProcess,
                              SourceCall<Object[], RecordReader> sourceCall) {
        Object[] pair =
                new Object[]{sourceCall.getInput().createKey(), sourceCall.getInput().createValue()};

        sourceCall.setContext(pair);

        try {
            // Skip the header
            sourceCall.getInput().next(pair[0],pair[1]);
        } catch (IOException e) {
            throw new RuntimeException("Could not skip the header");
        }
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
        throw new NotImplementedException("Storing to Splunk not implemented");
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
    public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
        throw new NotImplementedException("Storing to Splunk not implemented");
    }
}
