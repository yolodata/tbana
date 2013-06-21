package com.yolodata.tbana.hadoop.mapred.splunk.inputformat;

import com.yolodata.tbana.hadoop.mapred.splunk.SplunkConf;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkInputFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.List;


class TestMapper extends MapReduceBase implements Mapper<LongWritable, List<Text>, LongWritable, Text> {

    @Override
    public void map(LongWritable key, List<Text> values, OutputCollector<LongWritable, Text> outputCollector, Reporter reporter) throws IOException {
        Text output = new Text(StringUtils.join(values, ","));
        outputCollector.collect(key, output);
    }
}

class SplunkTestRunner extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        JobConf jobConf = new JobConf(getConf());

        jobConf.set(SplunkConf.SPLUNK_USERNAME, "admin");
        jobConf.set(SplunkConf.SPLUNK_PASSWORD, "changeIt");
        jobConf.set(SplunkConf.SPLUNK_HOST, "localhost");
        jobConf.set(SplunkConf.SPLUNK_PORT, "8089");
        jobConf.set(SplunkConf.SPLUNK_EARLIEST_TIME, "2012-12-30T23:59:55.000");
        jobConf.set(SplunkConf.SPLUNK_LATEST_TIME, "2013-01-31T23:59:59.000");
        jobConf.set(SplunkConf.SPLUNK_SEARCH_QUERY, "search * sourcetype=\"moc3\" | table sourcetype,_raw");

        jobConf.set(SplunkInputFormat.INPUTFORMAT_MODE,args[0]);
        jobConf.setJarByClass(SplunkTestRunner.class);
        jobConf.setNumReduceTasks(0);
        jobConf.setMapperClass(TestMapper.class);

        jobConf.setInputFormat(SplunkInputFormat.class);
        jobConf.setOutputKeyClass(NullWritable.class);
        jobConf.setOutputValueClass(Text.class);

        TextOutputFormat.setOutputPath(jobConf ,new Path(args[1]));

        JobClient.runJob(jobConf);

        return 0;
    }

    public SplunkTestRunner(Configuration conf) {
        super(conf);
    }
}