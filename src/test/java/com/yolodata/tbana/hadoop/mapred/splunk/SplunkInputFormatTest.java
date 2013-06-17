package com.yolodata.tbana.hadoop.mapred.splunk;

import com.yolodata.tbana.hadoop.mapred.TestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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

import java.io.IOException;
import java.util.List;

public class SplunkInputFormatTest {

    private Path outputPath= new Path("build/testTmp");
    private FileSystem fs;

    @Before
    public void setUp() throws Exception {
        fs= FileSystem.get(new Configuration());
        fs.delete(outputPath, true);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testJobUsingSplunkInputFormat() throws Exception {

        boolean jobCompleted = runJob();
        assert(jobCompleted == true); // Means that the job successfully finished

        String outputContent = TestUtils.readMapReduceOutputFile(fs, outputPath);

        List<String> lines = TestUtils.getLinesFromString(outputContent);
        int actualEvents = lines.size()-1; //Remove one line due to header

        assert(actualEvents == 5);

        String[] expectedEndOfLines = {
                "_raw",
                "#<DateTime 2012-12-31T23:59:59.000Z> count=0",
                "#<DateTime 2012-12-31T23:59:58.000Z> count=1",
                "#<DateTime 2012-12-31T23:59:57.000Z> count=2",
                "#<DateTime 2012-12-31T23:59:56.000Z> count=3",
                "#<DateTime 2012-12-31T23:59:55.000Z> count=4"};

        // Check that the last column of each line ends with the expected values
        for(int i=0;i<lines.size();i++)
            assert(lines.get(i).endsWith(expectedEndOfLines[i]));

    }

    private boolean runJob() throws Exception {
        return (ToolRunner.run(new Configuration(), new SplunkTestRunner(),
                new String[]{outputPath.toString()}) == 0);
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

        jobConf.set(ExportRecordReader.SPLUNK_USERNAME, "admin");
        jobConf.set(ExportRecordReader.SPLUNK_PASSWORD, "changeIt");
        jobConf.set(ExportRecordReader.SPLUNK_HOST, "localhost");
        jobConf.set(ExportRecordReader.SPLUNK_PORT, "8089");
        jobConf.set(ExportRecordReader.SPLUNK_EARLIEST_TIME, "2012-12-30T23:59:55.000");
        jobConf.set(ExportRecordReader.SPLUNK_LATEST_TIME, "2013-01-31T23:59:59.000");
        jobConf.set(ExportRecordReader.SPLUNK_SEARCH_QUERY, "search * sourcetype=\"moc3\"");

        jobConf.setJarByClass(SplunkTestRunner.class);
        jobConf.setNumReduceTasks(0);
        jobConf.setMapperClass(TestMapper.class);

        jobConf.setInputFormat(ExportInputFormat.class);
        jobConf.setOutputKeyClass(NullWritable.class);
        jobConf.setOutputValueClass(Text.class);

        TextOutputFormat.setOutputPath(jobConf ,new Path(args[0]));

        JobClient.runJob(jobConf);

        return 0;
    }

    public SplunkTestRunner() {
        super(new Configuration());
    }

}

