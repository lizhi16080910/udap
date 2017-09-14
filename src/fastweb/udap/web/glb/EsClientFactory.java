package fastweb.udap.web.glb;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;


public abstract class EsClientFactory {

	private static final String DEFAULT_IP = "192.168.100.3";
	private static final String IP_55 = "192.168.100.55";
	
	private static final String IP_KEY = "es.tcp_server";
	private static String ip = "192.168.100.3";

	public final static Client createClient() {
		
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
