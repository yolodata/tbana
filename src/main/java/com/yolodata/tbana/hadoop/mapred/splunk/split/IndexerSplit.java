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

}
