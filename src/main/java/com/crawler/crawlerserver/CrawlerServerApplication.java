package com.crawler.crawlerserver;

import com.google.common.collect.Queues;
import org.apache.nutch.fetcher.FetchNodeDb;
import org.apache.nutch.service.ConfManager;
import org.apache.nutch.service.JobManager;
import org.apache.nutch.service.NutchServer;
import org.apache.nutch.service.SeedManager;
import org.apache.nutch.service.impl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class CrawlerServerApplication {
	private final Logger logger = LoggerFactory.getLogger(CrawlerServerApplication.class);

	private static final int JOB_CAPACITY = 100;

	private long started;
	private boolean running;
	private ConfManager configManager;
	private JobManager jobManager;
	private SeedManager seedManager;

	private static FetchNodeDb fetchNodeDb;

	private static CrawlerServerApplication server;

	static {
		server = new CrawlerServerApplication();
	}

	public CrawlerServerApplication() {
		logger.info("Crawler server init");
		configManager = new ConfManagerImpl();
		seedManager = new SeedManagerImpl();
		BlockingQueue<Runnable> runnables = Queues.newArrayBlockingQueue(JOB_CAPACITY);
		NutchServerPoolExecutor executor = new NutchServerPoolExecutor(10, JOB_CAPACITY, 1, TimeUnit.HOURS, runnables);
		jobManager = new JobManagerImpl(new JobFactory(), configManager, executor);
		fetchNodeDb = FetchNodeDb.getInstance();
	}

	public ConfManager getConfManager() {
		return configManager;
	}

	public JobManager getJobManager() {
		return jobManager;
	}

	public SeedManager getSeedManager() {
		return seedManager;
	}

	public FetchNodeDb getFetchNodeDb(){
		return fetchNodeDb;
	}

	public boolean isRunning(){
		return running;
	}

	public long getStarted(){
		return started;
	}

	public static void main(String[] args) {
		SpringApplication.run(CrawlerServerApplication.class, args);
	}
}
