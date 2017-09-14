package fastweb.udap.web;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import fastweb.udap.util.PropertiesConfig;

public abstract class EsClientFactory {

	private static final String DEFAULT_IP = "192.168.100.3";
	private static final String IP_55 = "192.168.100.55";
	
	private static final String IP_KEY = "es.tcp_server";
	private static String ip = "";

	public final static Client createClient() {
		if (ip == null || ip.equals("")) {
			ip = PropertiesConfig.getStringProperty(IP_KEY, DEFAULT_IP);
		}
		Settings settings = ImmutableSettings.settingsBuilder().put(
				"cluster.name", "elasticsearch_cdnlog").build();

		Client client = new TransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(ip, 9300));
		return client;
	}
	
	public final static Client createClient55() {
		
		Settings settings = ImmutableSettings.settingsBuilder().put(
				"cluster.name", "cdnlog_analysis_es_cluster").build();

		Client client = new TransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(IP_55, 9301));
		return client;
	}
}
