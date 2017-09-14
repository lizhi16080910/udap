package fastweb.udap.util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.elasticsearch.client.Client;

import fastweb.udap.web.EsClientFactory;

public abstract class ESUtil {

	public static boolean isIndexExists(String indexName, Client client) {
		try {
			return client.prepareExists(indexName).get().exists();
		} catch (Exception e) {
			return false;
		}
	}

	public static boolean isIndexExists(String indexNmae) {
		Client client = null;
		try {
			client = EsClientFactory.createClient();
			return isIndexExists(indexNmae, client);
		} catch (Exception e) {
			return false;
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}

	public static List<String> getExistIndices(List<String> indices) {
		List<String> result = new ArrayList<String>();
		Client client = null;
		try {
			client = EsClientFactory.createClient();
			for (String indexName : indices) {
				try {
					boolean isExists = isIndexExists(indexName, client);
					if (isExists) {
						result.add(indexName);
					}
				} catch (Exception e) {
					// 判断index 不存在，会出现异常。
				}
			}
		} catch (Exception e) {
			// 创建client 会出现异常
		} finally {
			if (client != null) {
				client.close();
			}
		}
		return result;
	}
	
	//根据开始时间，结束时间，返回时间范围内的index，按天划分
	public static String[] getIndicesFromtime(long begin,long end,String indexPrefix)
	{
		List<String> indicesSuffix = Time2Type.time2Indices(begin, end);
		List<String> indices = new ArrayList<String>();
	
		for(String index : indicesSuffix){
			indices.add(indexPrefix+"."+index);
		}
		/* add timeout index */
		indices.add(indexPrefix);
		
		return ESUtil.getExistIndices(indices).toArray(new String[0]);
	}
	
	//根据开始时间，结束时间，返回月份index
	public static String[] getIndicesFromMonth(long begin,long end,String indexPrefix)
	{
		String beginstr = new SimpleDateFormat("yyyyMM").format(new Date(begin*1000));
		String endstr = new SimpleDateFormat("yyyyMM").format(new Date(end*1000));
		String beginindex = indexPrefix+"."+beginstr;
		String endindex = indexPrefix+"."+endstr;
		if(beginstr.equals(endstr))
		{
			return new String[]{beginindex};
		}else{
			return new String[]{beginindex,endindex};
		}
	}
	
}
