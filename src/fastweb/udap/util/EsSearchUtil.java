package fastweb.udap.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

public class EsSearchUtil {
	public static Logger logger = Logger.getLogger(EsSearchUtil.class);
	public static Object getJsonResult(SearchResponse searchResponse, SearchResultType type) {
		if(type == SearchResultType.AGGREGATIONS) {
			return getEsResult(searchResponse);
		} else {
			List<Map <String,Object>> rst = new ArrayList<Map<String,Object>>();
		    Iterator<SearchHit> it = searchResponse.getHits().iterator();
			while (it.hasNext()) {
				SearchHit sh = it.next();
				rst.add(sh.getSource());
			}
			return rst;
		}
	}
	
	public static Map<String,Object> getEsResult(SearchResponse response) {
		List<Aggregation> aggList = response.getAggregations().asList();
		if( aggList.isEmpty()) {
			return null;
		} else {
			Map<String,Object> esResult = new TreeMap<String,Object>();
			for(Aggregation agg : aggList){
				esResult.put(agg.getName(), getAggResult(agg));
			}
			return esResult;
		}
	}
	public static Object getAggResult(Aggregation agg) {
	
			if(agg instanceof Terms) { /* 非 leaf aggregation ,此时返回结果中不需要该agg的名字*/
				List<Bucket> bucketList = ((Terms)agg).getBuckets();
				Map<String,Object> bucketMap = new HashMap<String,Object>();
				for(Bucket buck : bucketList) {
					bucketMap.put(buck.getKey(),getBuckResult(buck));
				}
				return bucketMap;
			} else if(agg instanceof Sum) { /* leaf Aggregation */
				Long sumValue = ((Double)((Sum)agg).getValue()).longValue();		
				return sumValue;
			} else {
				return null;
			}
	}
	
	private static Object getBuckResult(Bucket buck) { /* buck下一定有数据*/
		
		List<Aggregation> aggList = buck.getAggregations().asList();
		if(aggList.size() > 1) {
			Map<String,Object> buckRst = new TreeMap<String,Object>();
			for( Aggregation agg : aggList) {
				buckRst.put(agg.getName(),getAggResult(agg));
			}
			return buckRst;
		} else {
			return getAggResult(aggList.get(0)); /* 只有一个子aggregation */
		}
	}
	
	public static Map<String,Object> joinEsResult(Map<String,Object> first, Map<String,Object> second)
	{
		return joinMap(first,second);
	}
	
	public static Map<String,Object> joinMap(Map<String,Object> first, Map<String,Object> second)
	{
		Map<String,Object> rstMap = new TreeMap<String,Object>();
		
		/* join */
		for(String key : first.keySet()) {
			if(second.containsKey(key)) {
				Object joinedObj = JoinObj(first.get(key),second.get(key));
				if(null != joinedObj) {
					rstMap.put(key, joinedObj);
				}
			} else {
				rstMap.put(key, first.get(key));
			}
		}
		
		/* append */
		for(String key: second.keySet()) {
			if(!first.containsKey(key)) {
				rstMap.put(key, second.get(key));
			}
		}
		return rstMap;
	}
	
	@SuppressWarnings("unchecked")
	private static Object JoinObj(Object first, Object second) {
		if( first.getClass().equals(first.getClass())){
			if( first instanceof Long) {
				return joinLong((Long) first,(Long) second);
			} else if( first instanceof Map) {
				return joinMap((Map<String,Object>) first,(Map<String,Object>) second);
			} else {
				logger.info("Unsupported Join item!");
				return null;
			}
		}
		return null;
	}

	public static Long joinLong(Long first,Long second){
		return first+second;
	}
	
	public enum SearchResultType{
		AGGREGATIONS(),HITS();
	}
	
	public class Pair<K,V> {
		public K key;
		public V value;
		Pair(K key,V value){
			this.key = key;
			this.value = value;
		}
	}
}
