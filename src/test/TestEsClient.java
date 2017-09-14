package test;

import java.util.Iterator;

import org.elasticsearch.action.exists.ExistsResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

public class TestEsClient {
	@Test
	public void createESNode() {
		Settings settings = ImmutableSettings.settingsBuilder().put(
				"cluster.name", "elasticsearch_cdnlog").build();

		Client client = new TransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(
						"192.168.100.205", 9300));
		GetRequest get = new GetRequest("");
		get.id("16552");
		get.index("nohit.machine.count");
		GetResponse response = client.get(get).actionGet();
		System.out.println(response.getSourceAsString());
	}

	@Test
	public void createSearch() {
		Settings settings = ImmutableSettings.settingsBuilder().put(
				"cluster.name", "elasticsearch_cdnlog").build();

		Client client = new TransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(
						"192.168.100.205", 9300));
		SearchResponse response = client.prepareSearch("nohit.machine.count")
				.setQuery(
						QueryBuilders.boolQuery().must(
								QueryBuilders.termQuery("machine_id",
										"ctl_js_195_159")).must(
								QueryBuilders.termQuery("type", "1")).must(
								QueryBuilders.termQuery("timestamp",
										"1430120460"))).addSort("topn_num",
						SortOrder.ASC).addSort("topn_num", SortOrder.ASC)
				.setFrom(1).setSize(3).execute().actionGet();

		Iterator<SearchHit> it = response.getHits().iterator();
		while (it.hasNext()) {
			SearchHit sh = it.next();
			System.out.println(sh.getSourceAsString());
		}
	}
	@Test
	public void createIndexExist() {
		Settings settings = ImmutableSettings.settingsBuilder().put(
				"cluster.name", "elasticsearch_cdnlog").build();

		Client client = new TransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(
						"192.168.100.3", 9300));
		ExistsResponse response = client.prepareExists("cdnlog.ua,cdnlog").get();
		System.out.println(response.exists());
	}
}
