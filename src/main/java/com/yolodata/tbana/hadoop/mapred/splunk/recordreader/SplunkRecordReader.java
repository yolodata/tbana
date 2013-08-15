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

package com.yolodata.tbana.hadoop.mapred.splunk.recordreader;


import com.splunk.Service;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkConf;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkService;
import com.yolodata.tbana.hadoop.mapred.splunk.split.SplunkSplit;
import com.yolodata.tbana.hadoop.mapred.util.ArrayListTextWritable;
import com.yolodata.tbana.hadoop.mapred.util.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

public abstract class SplunkRecordReader implements RecordReader<LongWritable, ArrayListTextWritable> {

    protected final SplunkConf configuration;
    protected long currentPosition;
    protected long startPosition;
    protected long endPosition;
    protected Service splunkService;

    protected InputStream is;
    protected InputStreamReader in;
    protected CSVReader reader;

    public SplunkRecordReader(SplunkConf configuration) throws IOException {

        this.configuration = configuration;
        this.configuration.validateConfiguration();

        splunkService = SplunkService.connect(configuration);

    }

    public abstract void initialize(InputSplit inputSplit) throws IOException;

    protected void initPositions(SplunkSplit inputSplit) {
        startPosition = inputSplit.getStart();
        endPosition = inputSplit.getEnd();
        currentPosition = startPosition;
    }

    @Override
    public boolean next(LongWritable key, ArrayListTextWritable value) throws IOException {

        reader = new CSVReader(in);
        if(key == null) key = createKey();
        if(value == null) value = createValue();

        int bytesRead = reader.readLine(value);

        if(bytesRead == 0) {
            key = null;
            value= null;
            return false;
        }

        key.set(currentPosition++);
        return true;
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public ArrayListTextWritable createValue() {
        return new ArrayListTextWritable();
    }

    @Override
    public long getPos() throws IOException {
        return currentPosition;
    }

    @Override
    public void close() throws IOException {
        if(is!=null) {
            is.close();
            is=null;
        }

        if(in!=null) {
            in.close();
            in=null;
        }
    }

    @Override
    public float getProgress() throws IOException {
        if (startPosition == endPosition) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (currentPosition - startPosition) / (float) (endPosition - startPosition));
        }
    }

}
