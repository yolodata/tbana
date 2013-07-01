package com.yolodata.tbana.hadoop.mapred.splunk.inputformat;

import com.yolodata.tbana.TestConfigurations;
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
        JobConf jobConf = new JobConf(TestConfigurations.getConfigurationWithSplunkConfigured());

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