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

package com.yolodata.tbana.hadoop.mapred.splunk;

import com.splunk.Job;
import com.splunk.JobArgs;
import com.splunk.JobResultsArgs;
import com.splunk.Service;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class SplunkJob {

    private Job job;

    public SplunkJob(Job job) {
        this.job = job;
    }

    public static SplunkJob getSplunkJob(Service service, String jobId) {
        Job job = service.getJobs().get(jobId);
        return new SplunkJob(job);
    }

    public static SplunkJob createSplunkJob(Service service, SplunkConf conf) {
        conf.validateSearchConfiguration();

        String searchQuery = conf.get(SplunkConf.SPLUNK_SEARCH_QUERY);
        JobArgs jobArgs = getJobArgs(conf);
        Job job = service.getJobs().create(searchQuery, jobArgs);
        return new SplunkJob(job);
    }

    public void waitForCompletion(int refreshRate) {
        while (!job.isDone()) {
            try {
                Thread.sleep(refreshRate);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static JobArgs getJobArgs(Configuration configuration) {
        JobArgs jobArgs = new JobArgs();

        jobArgs.setLatestTime(configuration.get(SplunkConf.SPLUNK_LATEST_TIME));
        jobArgs.setEarliestTime(configuration.get(SplunkConf.SPLUNK_EARLIEST_TIME));

        return jobArgs;
    }

    public Job getJob() {
        return job;
    }

    public int getNumberOfResultsFromJob(SplunkConf conf) {
        Service service = job.getService();
        String searchString = job.getSearch();
        searchString = searchString.concat(" | stats count");
        SplunkConf newConfig = new SplunkConf(conf);
        newConfig.set(SplunkConf.SPLUNK_SEARCH_QUERY,searchString);

        SplunkJob getEvents = SplunkJob.createSplunkJob(service,newConfig);
        getEvents.waitForCompletion(100);

        JobResultsArgs resultsArgs = new JobResultsArgs();
        resultsArgs.setOutputMode(JobResultsArgs.OutputMode.CSV);
        resultsArgs.setFieldList(new String[] {"count"});

        BufferedReader br = new BufferedReader(new InputStreamReader(getEvents.getJob().getResults(resultsArgs)));
        try {
            br.readLine(); // Skip header
            return Integer.parseInt(br.readLine());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}