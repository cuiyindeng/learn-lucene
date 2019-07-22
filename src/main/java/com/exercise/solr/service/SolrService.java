package com.exercise.solr.service;

import com.exercise.solr.SearchConfig;
import com.exercise.solr.SolrServerFinder;
import com.exercise.solr.domain.Restaurants;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.MapSolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class SolrService {

    private final Logger log = LoggerFactory.getLogger(SolrService.class);
    @Autowired
    private SolrServerFinder solrServerFinder;
    private final String RESTAURANTS_COLLECTION_NAME = "restaurants";

    public void solrIndex() throws IOException, SolrServerException {
        HttpSolrClient httpSolrClient = getSolrClient();

        String fileUri = SearchConfig.class.getClassLoader()
                .getResource("restaurants.json")
                .getFile();
        log.info(fileUri);

        ObjectMapper mapper = new ObjectMapper();
        List<Restaurants> restaurantsList = mapper.readValue(new File(fileUri), new TypeReference<List<Restaurants>>() {});

        UpdateResponse response = httpSolrClient.addBeans(RESTAURANTS_COLLECTION_NAME, restaurantsList);
        log.info(response.toString());

        httpSolrClient.commit(RESTAURANTS_COLLECTION_NAME);
    }

    private HttpSolrClient getSolrClient() {
        String serverUrl = solrServerFinder.getServerUrl();
        log.info(serverUrl);

        return new HttpSolrClient.Builder(serverUrl)
                .withConnectionTimeout(10000)
                .withSocketTimeout(60000)
                .build();
    }

    public void solrSearch() throws IOException, SolrServerException {
        final HttpSolrClient client = getSolrClient();

        final Map<String, String> queryParamMap = new HashMap<String, String>();
        queryParamMap.put("q", "*:*");
//        queryParamMap.put("fq", "price:[5 TO 10]");
        queryParamMap.put("fl", "id, name, price");
        queryParamMap.put("sort", "id asc");
        MapSolrParams queryParams = new MapSolrParams(queryParamMap);

        //url decode: http://tool.chinaz.com/tools/urlencode.aspx
        log.info(queryParams.toQueryString());

        final QueryResponse response = client.query(RESTAURANTS_COLLECTION_NAME, queryParams);
        final SolrDocumentList documents = response.getResults();

        log.info("Found " + documents.getNumFound() + " documents");
        for(SolrDocument document : documents) {
            final String id = (String) document.getFirstValue("id");
            final String name = (String) document.getFirstValue("name");
            final Double price = (Double) document.getFirstValue("price");

            log.info("id: " + id + "; name: " + name + "; price: " + price);
        }
    }
}
