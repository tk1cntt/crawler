package com.crawler.crawlerserver;

import com.google.common.collect.Queues;
import org.apache.nutch.fetcher.FetchNodeDb;
import org.apache.nutch.service.ConfManager;
import org.apache.nutch.service.JobManager;
import org.apache.nutch.service.NutchServer;
import org.apache.nutch.service.SeedManager;
import org.apache.nutch.service.impl.*;
import org.apache.nutch.service.model.request.JobConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class CrawlerServerApplication {
	private final Logger logger = LoggerFactory.getLogger(CrawlerServerApplication.class);
	private static final int JOB_CAPACITY = 100;
	private static FetchNodeDb fetchNodeDb;

	@Bean
	public ConfManager getConfigManager() {
		return new ConfManagerImpl();
	}

	@Bean
	public JobManager getJobManager() {
		BlockingQueue<Runnable> runnables = Queues.newArrayBlockingQueue(JOB_CAPACITY);
		NutchServerPoolExecutor executor = new NutchServerPoolExecutor(10, JOB_CAPACITY, 1, TimeUnit.HOURS, runnables);
		return new JobManagerImpl(new JobFactory(), getConfigManager(), executor);
	}

	@Bean
	public SeedManager getSeedManager() {
		return new SeedManagerImpl();
	}

	@Bean
	public FetchNodeDb getFetchNodeDb(){
		return FetchNodeDb.getInstance();
	}

	public static void main(String[] args) {
		SpringApplication.run(CrawlerServerApplication.class, args);
	}
}
