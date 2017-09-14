package fastweb.udap.util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.elasticsearch.client.Client;

import fastweb.udap.web.EsClientFactory;

public class Time2Type {
	public static SimpleDateFormat ymdhFomat = new SimpleDateFormat("yyyyMMddHH");
	public static SimpleDateFormat ymdFomat = new SimpleDateFormat("yyyyMMdd");

	public static List<String> time2Types(long begin,long end,String metric)
	{
		List<String> rstList = new ArrayList<String>();
		String[] parts = metric.split("\\.");
		
		if(parts[parts.length-1].equalsIgnoreCase("one")){ /* freqence set to hour */
			rstList=geneTimeString(begin,end,Freqence.HOUR);
		} else{											   /* freqence set to day */
			rstList=geneTimeString(begin,end,Freqence.DAY);
		}
		return rstList;
	}
	
	private static boolean isIndexExists(String indexName,Client client) {
		try {
		  return client.prepareExists(indexName).get().exists();
		} catch (Exception e){
			return false;
		}
	}
	
	public static List<String> time2Indices( long begin, long end){
		return geneTimeString(begin,end,Freqence.DAY);
	}
	
	public static List <String> indicesExistsCheck(List<String> indices) {
		
		List<String > rst = new ArrayList<String>();
		
		Client client = null ;
		try {
			client = EsClientFactory.createClient();
			for(String str : indices) {
				if(isIndexExists(str,client)) {
					rst.add(str);
				}
			}
		} catch (Exception e){
			e.printStackTrace();
		}  finally {
			if( null != client) {
				client.close();
			}
		}
		return rst;
	}
	
	private static List<String> geneTimeString(long begin,long end,Freqence freq)
	{
		List<String> rstList = new ArrayList<String>();
		Calendar beginCal = Calendar.getInstance();
		int gmtOffset = TimeZone.getTimeZone("GMT+08").getOffset(begin);
		/* round the timestamp according to freq type */
		switch(freq){
			case DAY:
				begin = (begin+gmtOffset)/(3600*24)*3600*24-gmtOffset+1;
				end = (end+gmtOffset)/(3600*24)*3600*24-gmtOffset+10;
				break;
			case HOUR:
				begin = begin/(3600)*3600+1;
				end = end/(3600)*3600+10;
				break;
			default:
				break;
		}
		beginCal.setTime(new Date(begin*1000));
		Calendar endCal = Calendar.getInstance();
		endCal.setTime(new Date(end*1000));
		while(beginCal.before(endCal)) {
			switch(freq){
				case DAY:
					rstList.add(ymdFomat.format(beginCal.getTime()));
					beginCal.add(Calendar.DATE, 1);
					break;
				case HOUR:
					rstList.add(ymdhFomat.format(beginCal.getTime()));
					beginCal.add(Calendar.HOUR_OF_DAY, 1);
					break;
				default:
					break;
			}
		}
		
		return rstList;
	}
	
	public enum Freqence{
		Month("month"),DAY("day"),HOUR("hour");
		
		private String freqStr;
		
		private Freqence(String freqStr) {
			this.freqStr = freqStr;
		}
		
		private String getFreqStr()
		{
			return this.freqStr;
		}
	}
	
	public static void main(String[] args)
	{
		
		List<String> rstList = time2Types(1433865600l, 1433952000l, "status.code.domain.one");
		String[] rst = rstList.toArray(new String[0]);
		System.out.println("Time String List As Follow:");
		for(String str:rst){
			System.out.println(str);
		}
	}
}
