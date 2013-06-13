package com.yolodata.tbana.hadoop.mapred.splunk;

import com.yolodata.tbana.hadoop.mapred.CSVNLineInputFormat;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class SplunkInputFormatTest {

    String outputPath= "build/testTmp";

    @Before
    public void setUp() throws Exception {
        FileUtils.deleteDirectory(new File(outputPath));
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testJobUsingSplunkInputFormat() throws Exception {

        boolean jobCompleted = runJob();
        assert(jobCompleted == true); // Means that the job successfully finished

    }

    private boolean runJob() throws Exception {
        return (ToolRunner.run(new Configuration(), new SplunkTestRunner(),
                new String[]{outputPath}) == 0);
    }
}


class TestMapper extends MapReduceBase implements Mapper<LongWritable, List<Text>, LongWritable, Text> {

    @Override
    public void map(LongWritable key, List<Text> values, OutputCollector<LongWritable, Text> outputCollector, Reporter reporter) throws IOException {
        Text output = new Text(StringUtils.join(values,","));
        outputCollector.collect(key, output);
    }
}

class SplunkTestRunner extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        JobConf jobConf = new JobConf(getConf());

        jobConf.set(SplunkExportRecordReader.SPLUNK_USERNAME, "admin");
        jobConf.set(SplunkExportRecordReader.SPLUNK_PASSWORD, "changeIt");
        jobConf.set(SplunkExportRecordReader.SPLUNK_HOST, "localhost");
        jobConf.set(SplunkExportRecordReader.SPLUNK_PORT, "8089");
        jobConf.set(SplunkExportRecordReader.SPLUNK_SEARCH_QUERY, "search source=/var/log/system.log");

        jobConf.setJarByClass(SplunkTestRunner.class);
        jobConf.setNumReduceTasks(0);
        jobConf.setMapperClass(TestMapper.class);

        jobConf.setInputFormat(SplunkInputFormat.class);
        jobConf.setOutputKeyClass(NullWritable.class);
        jobConf.setOutputValueClass(Text.class);

        TextOutputFormat.setOutputPath(jobConf,new Path(args[0]));

        JobClient.runJob(jobConf);

        return 0;
    }

    public SplunkTestRunner() {
        super(new Configuration());
    }

}

