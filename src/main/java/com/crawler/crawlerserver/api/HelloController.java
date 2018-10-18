package com.crawler.crawlerserver.api;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.nutch.service.JobManager;
import org.apache.nutch.service.SeedManager;
import org.apache.nutch.service.impl.JobWorker;
import org.apache.nutch.service.model.request.JobConfig;
import org.apache.nutch.service.model.request.SeedList;
import org.apache.nutch.service.model.request.SeedUrl;
import org.apache.nutch.service.model.response.JobInfo;
import org.apache.nutch.util.NutchTool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.OutputStream;
import java.util.*;

@RestController
public class HelloController {

    @Autowired
    private SeedManager seedManager;

    @Autowired
    private JobManager jobManager;

    private String writeToSeedFile(Collection<SeedUrl> seedUrls) throws Exception {
        String seedFilePath = "seedFiles/seed-" + System.currentTimeMillis();
        org.apache.hadoop.fs.Path seedFolder = new org.apache.hadoop.fs.Path(seedFilePath);
        FileSystem fs = FileSystem.get(new Configuration());
        if (!fs.exists(seedFolder)) {
            if (!fs.mkdirs(seedFolder)) {
                throw new Exception("Could not create seed folder at : " + seedFolder);
            }
        }
        String filename = seedFilePath + System.getProperty("file.separator") + "urls";
        org.apache.hadoop.fs.Path seedPath = new org.apache.hadoop.fs.Path(filename);
        OutputStream os = fs.create(seedPath);
        if (CollectionUtils.isNotEmpty(seedUrls)) {
            for (SeedUrl seedUrl : seedUrls) {
                os.write(seedUrl.getUrl().getBytes());
                os.write("\n".getBytes());
            }
        }
        os.close();
        return seedPath.getParent().toString();
    }

    @RequestMapping("/")
    public JobInfo index() throws Exception {
        List<SeedUrl> urls = new ArrayList<>();
        urls.add(new SeedUrl("http://nutch.apache.org"));
        String seedPath = writeToSeedFile(urls);
        JobConfig config = new JobConfig();
        config.setCrawlId("crawl-01");
        config.setConfId("default");
        config.setType(JobManager.JobType.INJECT);
        Map<String, Object> args = new HashMap<>();
        args.put("url_dir", seedPath);
        config.setArgs(args);
        return jobManager.create(config);
    }

}
