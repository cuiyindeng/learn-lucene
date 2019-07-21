package com.exercise.solr;

import com.exercise.solr.service.ZooKeeperSolrServerFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class SearchConfig {

	@Value("${zookeeper.servers:NONE}")
	private String zookeeperServerList;
	@Value("${solr.server:localhost}")
	private String solrServer;

	@Autowired
	private SolrServerFinder solrServerFinder;

	private final String RESTAURANTS_COLLECTION_NAME = "restaurants";

	private final Logger log = LoggerFactory.getLogger(SearchConfig.class);

	@Bean(name = "solrServerFinder")
	public SolrServerFinder chemicalSolrServerFinder() {
		if ("NONE".equalsIgnoreCase(zookeeperServerList)) {
//			return new LocalSolrServerFinder(solrServer, COLLECTION_NAME);
			return null;
		} else {
			log.debug("Using clustered solr chemical instance:");
			return new ZooKeeperSolrServerFinder(zookeeperServerList, RESTAURANTS_COLLECTION_NAME);
		}
	}

	@RequestMapping("/getSolrCloud")
	public String getSolrCloud() {
		log.info(solrServerFinder.getServerEndpoint());
		return "success";
	}


	public static void main(String... args) {
		SpringApplication.run(SearchConfig.class, args);
	}
}
