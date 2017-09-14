//package test;
//
//import java.util.HashMap;
//import java.util.Map;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.elasticsearch.action.count.CountRequest;
//import org.elasticsearch.action.search.SearchResponse;
//import org.elasticsearch.action.search.SearchType;
//import org.elasticsearch.client.Client;
//import org.elasticsearch.client.transport.TransportClient;
//import org.elasticsearch.common.transport.InetSocketTransportAddress;
//import org.elasticsearch.index.query.FilterBuilders;
//import org.elasticsearch.index.query.QueryBuilder;
//import org.elasticsearch.index.query.QueryBuilders;
//import org.elasticsearch.search.SearchHit;
//import org.elasticsearch.search.SearchHits;
//import org.elasticsearch.search.facet.FacetBuilder;
//import org.elasticsearch.search.facet.FacetBuilders;
//
//
//
//public class SearchTrafficAPI {
//    
//    public static final Log log = LogFactory.getLog(SearchTrafficAPI.class);
//    private Client client;
//    
//    public static SearchTrafficAPI instance = new SearchTrafficAPI();
//    
//    public  SearchTrafficAPI() {
//        client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("192.168.100.72", 9300));
//    }
//    
//    public static void main(String[] args) throws InterruptedException {
////        new SearchTrafficAPI().SearchAPI.searchByQuery();
//        new SearchTrafficAPI().searchByFilter();
//    }
//    
//    public void searchByQuery() {
//        long startTime = System.currentTimeMillis();
//        QueryBuilder query = QueryBuilders.boolQuery()
////                                        .must(QueryBuilders.matchPhraseQuery("time", "1414008000"))
//                                        .must(QueryBuilders.termsQuery("tag.prv", "867740", "869430", "865310", "865710", "868120", "868919", "869370"))
//                                        .must(QueryBuilders.termsQuery("tag.domain", "upext.chrome.360.cn", "pic.moretv.com.cn"))
//                                        ; 
//        
//        //测试聚合，不成功
////        FacetBuilder fcb = FacetBuilders.termsFacet("tag.prv").field("tag.prv")
////                                        .facetFilter(FilterBuilders.queryFilter(
////                                                                   QueryBuilders.boolQuery().must(QueryBuilders.matchPhraseQuery("tag.machine", "CTL_JS_002_019"))));
//        
//        SearchResponse response = client.prepareSearch(TrafficIndexThread.index).setTypes(TrafficIndexThread.type)
//                                        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                                        .setQuery(query)
////                                        .addFacet(fcb)
//                                        .setFrom(0).setSize(Integer.MAX_VALUE)
//                                        .execute().actionGet();
//        
//        
//        //SearchHits是SearchHit的复数形式，表示这个是一个列表
//        SearchHits hits = response.getHits();
//        Long totalValue = 0L;
//        for(SearchHit hit : hits){
//            System.out.println("id:" + hit.getId() + ":" + hit.getSourceAsString());
////            System.out.println("------------"+ hit.getFields().get("tag.domain").getValue());    //报错
//            if (hit.getSource().get("value") != null) {
//                Object o = hit.getSource().get("value");
//                if (o instanceof Integer) {
//                    totalValue = totalValue + (Integer)o;
//                } else if (o instanceof Long) {
//                    totalValue = totalValue + (Long)o;
//                }
//            }
//        }
//        log.info("hit count=" + hits.getTotalHits());
//        log.info("totalValue=" + totalValue);
//        log.info("spent time:" + (System.currentTimeMillis() - startTime));
//    }
//    
//    public void searchByFilter() {
//        long startTime = System.currentTimeMillis();
//        
////      FacetBuilder fcb = FacetBuilders.termsFacet("tag.prv").field("tag.prv")
//        
//        SearchResponse response = client.prepareSearch(TrafficIndexThread.index).setTypes(TrafficIndexThread.type)
//                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
////                .setQuery(QueryBuilders.termsQuery("tag.domain", "js.haiyunx.com", "tvhd.fw.live.cntv.cn"))     // Query
////                .setPostFilter(FilterBuilders.andFilter(FilterBuilders.termsFilter("tag.prv", "867740", "865930"),   // Filter  
////                                                        FilterBuilders.termsFilter("tag.isp", "11"),
////                                                        FilterBuilders.termsFilter("tag.domain", "js.haiyunx.com", "tvhd.fw.live.cntv.cn")
////                                                        )
////                               )
//                .setFrom(0).setSize(Integer.MAX_VALUE)
//                .setExplain(true).execute().actionGet();
//
//        //SearchHits是SearchHit的复数形式，表示这个是一个列表
//        SearchHits hits = response.getHits();
//        Long totalValue = 0L;
//        for(SearchHit hit : hits){
//            System.out.println("id:" + hit.getId() + ":" + hit.getSourceAsString());
//            
//            if (hit.getSource().get("value") != null) {
//                Object o = hit.getSource().get("value");
//                if (o instanceof Integer) {
//                    totalValue = totalValue + (Integer)o;
//                } else if (o instanceof Long) {
//                    totalValue = totalValue + (Long)o;
//                }
//            }
//        }
//        log.info("hit count=" + hits.getTotalHits());
//        log.info("totalValue=" + totalValue);
//        log.info("spent time:" + (System.currentTimeMillis() - startTime));
//    }
//    
//    public long count() {
//        return this.client.count(new CountRequest(TrafficIndexThread.index).types(TrafficIndexThread.type)).actionGet().getCount();
//    }
//}
