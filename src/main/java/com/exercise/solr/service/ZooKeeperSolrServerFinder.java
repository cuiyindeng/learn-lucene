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
			log.debug("1........Checking for live solr nodes.");
			zk.exists("/live_nodes", new Executor());
		} catch (Exception ex) {
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

	@Override
	public String getServerUrl() {
		int serverChoice = random.nextInt(availableServers.size());
		final SolrServerInfo server = availableServers.get(serverChoice);
		log.debug("10........Selected solr server url: {}", server.server);
		return server.server;
	}

	private class Executor implements Watcher, AsyncCallback.DataCallback {

		private final ObjectMapper mapper = new ObjectMapper();

		/**
		 * Watcher接口的方法，用来接收Zookeeper Server触发的Watcher事件通知
		 * @param we
		 */
		@Override
		public void process(WatchedEvent we) {
			log.debug("2........call process method, path:{}, type:{}", we.getPath(), we.getType());
			//第一次连接时，EventType为None (-1)
			if (we.getType() == Event.EventType.None) {
				log.debug("3-1........Waiting for connection. Current state: {}", we.getState());
				if (we.getState() == Event.KeeperState.SyncConnected) {
					try {
						zk.getData("/collections/" + collectionName + "/state.json", this, this, null);
					} catch (Exception ex) {
						throw new RuntimeException();
					}
				}
			} else {
				/**
				 当zookeeper Server触发的EventType为
				 NodeCreated (1),
				 NodeDeleted (2),
				 NodeDataChanged (3),
				 NodeChildrenChanged (4);
				时会执行此逻辑。也就是说Solr Cloud的某个Node有变动时，此逻辑也会执行。
				 */
				log.debug("3-2........Event for {}/{}", we.getPath(), we.getType());
				zk.getData("/collections/" + collectionName + "/state.json", this, this, null);
			}
		}

		/**
		 * AsyncCallback.DataCallback接口的方法，异步接口通常用在：应用启动的时候，获取一些配置信息，例如“机器列表”
		 * @param resultCode
		 * @param path
		 * @param ctx
		 * @param bytes
		 * @param stat
		 */
		@Override
		public void processResult(int resultCode, String path, Object ctx, byte[] bytes, Stat stat) {
			log.debug("4........Data changed for path: {}/{}", path, resultCode);
			StringBuilder bld = new StringBuilder();
			for (byte aByte : bytes) {
				bld.append((char) aByte);
			}
			try {
				final String rawJson = bld.toString();
				log.debug("5........Current raw path info is: {}", rawJson);
				JsonNode data = mapper.readTree(rawJson);
				JsonNode jsonConfig = data.get(collectionName);
				JsonNode shardsAvailable = jsonConfig.get("shards");
				Iterator<String> shardNames = shardsAvailable.fieldNames();
				List<SolrServerInfo> newServers = new ArrayList<>();
				while (shardNames.hasNext()) {
					String shardName = shardNames.next();
					log.debug("6........Found shard: {}", shardName);
					JsonNode shardReplicas = shardsAvailable.get(shardName).get("replicas");
					Iterator<String> replicaNames = shardReplicas.fieldNames();
					while (replicaNames.hasNext()) {
						JsonNode replica = shardReplicas.get(replicaNames.next());
						final String state = replica.get("state").textValue();
						final String url = replica.get("base_url").textValue();
						final String forCore = replica.get("core").textValue();
						log.debug("7........Found server {}/{}, it's current state is: {}", new Object[]{url,forCore, state});
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

			log.debug("8........Found {} active server(s)", availableServers.size());
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
