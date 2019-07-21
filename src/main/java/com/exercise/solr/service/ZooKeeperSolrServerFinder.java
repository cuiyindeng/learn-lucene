package com.exercise.solr.service;

import com.exercise.solr.SolrServerFinder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.*;

/**
 * 使用zookeeper找到solr cloud中指定的collection
 */
public class ZooKeeperSolrServerFinder implements SolrServerFinder {

	private final Logger log = LoggerFactory.getLogger(ZooKeeperSolrServerFinder.class);
	private final String zooKeeperServerList;
	private final List<SolrServerInfo> availableServers = Collections.synchronizedList(new ArrayList<SolrServerInfo>());

	private String collectionName;

	private ZooKeeper zk;

	private final Random random = new Random(new Date().getTime());

	public ZooKeeperSolrServerFinder(String zooKeeperServerList, String collectionName) {
		this.zooKeeperServerList = zooKeeperServerList;
		this.collectionName = collectionName;
	}

	@PostConstruct
	public void setup() {
		try {
			zk = new ZooKeeper(zooKeeperServerList, 3000, new Executor());
			log.debug("Checking for live solr nodes.");
			zk.exists("/live_nodes", new Executor());
		} catch (IOException | KeeperException | InterruptedException ex) {
			throw new RuntimeException(ex);
		}
	}

	@PreDestroy
	public void destroy() {
		try {
			zk.close();
		} catch (Exception ex) {
			log.debug("Error closing solr connection.", ex);
		}
	}


	@Override
	public String getServer() {
		int serverChoice = random.nextInt(availableServers.size());
		final SolrServerInfo server = availableServers.get(serverChoice);
		log.debug("Selected solr server: {}/{}", server.server, server.core);
		return server.server + "/" + server.core;
	}

	@Override
	public String getServerEndpoint() {
		return getServer().replace("http", "solr") + "?soTimeout=5000";
	}

	private class Executor implements Watcher, AsyncCallback.DataCallback {

		private final ObjectMapper mapper = new ObjectMapper();

		@Override
		public void process(WatchedEvent we) {
			if (we.getType() == Event.EventType.None) {
				log.debug("Waiting for connection. Current state: {}", we.getState());
				if (we.getState() == Event.KeeperState.SyncConnected) {
					try {
						zk.getData("/collections/" + collectionName + "/state.json", this, this, null);
					} catch (Exception ex) {
						throw new RuntimeException();
					}
				}
			} else {
				log.debug("Event for {}/{}", we.getPath(), we.getType());
				zk.getData("/collections/" + collectionName + "/state.json", this, this, null);
			}
		}

		@Override
		public void processResult(int resultCode, String path, Object ctx, byte[] bytes, Stat stat) {
			log.debug("Data changed for path: {}/{}", path, resultCode);
			StringBuilder bld = new StringBuilder();
			for (byte aByte : bytes) {
				bld.append((char) aByte);
			}
			try {
				final String rawJson = bld.toString();
				log.debug("Current raw path info is: {}", rawJson);
				JsonNode data = mapper.readTree(rawJson);
				JsonNode jsonConfig = data.get(collectionName);
				JsonNode shardsAvailable = jsonConfig.get("shards");
				Iterator<String> shardNames = shardsAvailable.fieldNames();
				List<SolrServerInfo> newServers = new ArrayList<>();
				while (shardNames.hasNext()) {
					String shardName = shardNames.next();
					log.debug("Found shard: {}", shardName);
					JsonNode shardReplicas = shardsAvailable.get(shardName).get("replicas");
					Iterator<String> replicaNames = shardReplicas.fieldNames();
					while (replicaNames.hasNext()) {
						JsonNode replica = shardReplicas.get(replicaNames.next());
						final String state = replica.get("state").textValue();
						final String url = replica.get("base_url").textValue();
						final String forCore = replica.get("core").textValue();
						log.debug("Found server {}/{}, it's current state is: {}", new Object[]{url,forCore, state});
						if (state.equals("active")) {
							newServers.add(new SolrServerInfo(url, forCore));
						}
					}
				}
				availableServers.clear();
				availableServers.addAll(newServers);
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}

			log.debug("Found {} active server(s)", availableServers.size());
		}
	}
	
	static class SolrServerInfo {
		String server;
		String core;

		public SolrServerInfo(String server, String core) {
			this.server = server;
			this.core = core;
		}
	}
}
