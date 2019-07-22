package com.exercise.solr;

import com.exercise.solr.domain.Restaurants;
import com.exercise.solr.service.SolrService;
import com.exercise.solr.service.ZooKeeperSolrServerFinder;

import org.apache.solr.client.solrj.SolrServerException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@SpringBootApplication
@RestController
public class SearchConfig {

	@Value("${zookeeper.servers:NONE}")
	private String zookeeperServerList;
	@Value("${solr.server:localhost}")
	private String solrServer;
	@Autowired
	private SolrService solrService;

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

	@RequestMapping("/testSolrCloud")
	public String testSolrCloud() throws IOException, SolrServerException {
//		solrService.solrIndex();
		solrService.solrSearch();
		return "success";
	}

	public static void main(String... args) {
		SpringApplication.run(SearchConfig.class, args);
	}
}
