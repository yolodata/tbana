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

package com.yolodata.tbana.hadoop.mapred.splunk.split;

import org.apache.hadoop.mapred.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SplunkSplit implements InputSplit{

    private String jobID;
    private long start;
    private long end;
    private boolean skipHeader;

    public SplunkSplit() {
    }

    public SplunkSplit(long start, long end) {
        this("NONE",start,end);
    }

    public SplunkSplit(String jobID, long start, long end) {
        this(jobID, start, end, false);
    }

    public SplunkSplit(String jobID, long start, long end, boolean skipHeader) {
        this.start = start;
        this.end = end;
        this.jobID = jobID;
        this.skipHeader = skipHeader;
    }

    @Override
    public long getLength() throws IOException {
        return end-start;
    }

    @Override
    public String[] getLocations() throws IOException {
        return new String[] {String.valueOf(start), String.valueOf(end)};
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(jobID);
        dataOutput.writeLong(start);
        dataOutput.writeLong(end);
        dataOutput.writeBoolean(skipHeader);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.jobID = dataInput.readUTF();
        this.start = dataInput.readLong();
        this.end = dataInput.readLong();
        this.skipHeader = dataInput.readBoolean();
    }

    public long getStart() {
        return this.start;
    }

    public long getEnd() {
        return this.end;
    }

    public String getJobID() {
        return this.jobID;
    }

    public boolean getSkipHeader() {
        return skipHeader;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj==null || obj.getClass() != getClass())
            return false;

        SplunkSplit split = (SplunkSplit) obj;

        return  this.jobID.equals(split.jobID) &&
                this.start == split.start &&
                this.end == split.end &&
                this.skipHeader == split.skipHeader;
    }
}
