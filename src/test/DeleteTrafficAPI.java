package test;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.facet.FacetBuilder;
import org.elasticsearch.search.facet.FacetBuilders;



public class DeleteTrafficAPI {
    
    public static final Log log = LogFactory.getLog(DeleteTrafficAPI.class);
    private Client client;
    
    public static DeleteTrafficAPI instance = new DeleteTrafficAPI();
    
    public  DeleteTrafficAPI() {
        client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("192.168.100.72", 9300));
    }
    
    public static void main(String[] args) throws InterruptedException {
        new DeleteTrafficAPI().deleteIndex();
    }
    
    public void deleteIndex() {
//        DeleteResponse response = client.prepareDelete("twitter", "tweet", "1").execute().actionGet();
        
        QueryBuilder query = QueryBuilders.termQuery("time", "1414008000");  
        client.prepareDeleteByQuery("cdnlog.traffic").setQuery(query).execute().actionGet();  
    }
    
    public void delete() {
        // delete by id
        DeleteResponse response = client
                .prepareDelete("cdnlog.traffic","traffic", "1").execute()
                .actionGet();

        // delete by query
        QueryBuilder query = QueryBuilders.fuzzyQuery("title", "query"); 
        DeleteByQueryResponse res = client.prepareDeleteByQuery("traffic").setQuery(query).execute().actionGet();
        int status = res.status().getStatus();
    }
    
}
