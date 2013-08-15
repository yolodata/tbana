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

import com.yolodata.tbana.hadoop.mapred.splunk.indexer.Indexer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IndexerSplit extends SplunkSplit{

    private Indexer indexer;

    public IndexerSplit() {

    }

    public IndexerSplit(Indexer indexer, String jobID, long start, long end, boolean skipHeader) {
        super(jobID,start,end,skipHeader);

        this.indexer = indexer;
    }

    public Indexer getIndexer() {
        return indexer;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);

        dataOutput.writeUTF(indexer.getHost());
        dataOutput.writeInt(indexer.getPort());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);

        this.indexer = new Indexer(dataInput.readUTF(),dataInput.readInt());
    }

    @Override
    public boolean equals(Object obj) {
        if(obj==null || obj.getClass() != getClass())
            return false;

        IndexerSplit split = (IndexerSplit) obj;
        return this.indexer.equals(split.indexer) &&
                super.equals(obj);
    }
}
