package fastweb.udap.web.action;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.struts2.ServletActionContext;
import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.bean.Tag;
import fastweb.udap.cache.CacheLoader;
import fastweb.udap.cache.Constants;
import fastweb.udap.http.EsRequestBalance;
import fastweb.udap.http.PostRequestHander;
import fastweb.udap.http.RequestVO;
import fastweb.udap.util.DatetimeUtil;
import fastweb.udap.util.FileHelper;
import fastweb.udap.util.StringUtil;


/**
 * 名称: BigdataQueryAction.java
 * 描述: 
 * 最近修改时间: Nov 7, 20142:14:06 PM
 * @since Nov 7, 2014
 * @author zhangyi
 */
public class BigdataQueryAction extends ActionSupport {
 
	private static final long serialVersionUID = 1L;
	private final Log log = LogFactory.getLog(getClass());
	private HttpServletRequest request = ServletActionContext.getRequest();
	private HttpServletResponse response = ServletActionContext.getResponse();

	private static final int DEFAULT_QUERY_NUM = Integer.MAX_VALUE;
	private static final int FROM_QUERY_NUM = 0;
	private String index;
	private String type;
	private static final String action = "_search";
	private static byte[] aggsJsonBytes = FileHelper.read(new File(Constants.SEARCH_AGGS_SUM));
	private String aggsJson = new String(aggsJsonBytes);

	private String start;
	private String end;
	private String from;
	private String size;
	private String group;
	private String limit;
	private String sumField;
	private String sort;

	private String q;
	private Tag tag = new Tag();
	private String pretty;
    private RequestVO vo;

	@Override
	public String execute() {
		response.setContentType("application/json;charset=UTF-8");   //解决中文乱码
		response.setCharacterEncoding("UTF-8");
		try {
			receiveParams();
			
			/*跳转statuscodecountAction*/
//			if ("cdnlog.status.url".equals(this.index)) {
//				request.getRequestDispatcher("/statuscodecount.action").forward(request, response);
//				return null;
//			}
			
			String errCode = checkParam();
			if (!errCode.equals("0000")) {
				response.getWriter().write("{\"error\":{\"code\":" + errCode + ",\"message\":\"" + CacheLoader.getRespCodeMap().get(errCode) + "\"}}");
				return null;
			}
			
			String url = getFullUrl();
	        log.info("ES url=" + url);
	        if (vo != null) {
	            log.info("aggsJson=" + vo.getQueryString());
	        }
			String jsonResult = PostRequestHander.getContentByHttp(url, vo, true).getRespContent();
            if (StringUtil.isEmpty(jsonResult)) {
                response.getWriter().write("{\"error\":{\"code\":1000,\"message\":\"" + CacheLoader.getRespCodeMap().get("1000") + "\"}}");
            } else {
                response.getWriter().write(jsonResult);
            }
		} catch (Exception e) {
			log.error("QueryAction execute() error!" + e);
			e.printStackTrace();
			try {
				response.getWriter().write("{\"error\":{\"code\":0999,\"message\":\"" + CacheLoader.getRespCodeMap().get("0999") + "\"}}");
			} catch (IOException e1) {
			    e1.printStackTrace();
			}
		}
		return null;
	}
	
	/*
       状态码示例
       http://192.168.100.181:9200/cdnlog.status.url/201411/_search?size=1&q=(prv:865719 OR prv:867310) AND (domain:haochi.55tuan.com OR domain:v03.ysg.netease.com) AND (time:1416196800)
       http://192.168.100.181:8080/udap1.2/cdnlog/bigdataQuery.action?index=cdnlog.status.url&action=_search&size=1&start=1415754000&end=1415754000&q=(prv:865719 OR prv:867310)
	 */
	private String getFullUrl() {
	    String timeRangStr = null;
	    if (StringUtil.isEmpty(group) && StringUtil.isEmpty(sumField)) {
	        vo = null;
	        timeRangStr = "time:[" + start + " TO " + end + "]"; 
	    } else {
	        vo = getJsonGroupAgg();
	    }
        
        //拼装url中的get请求参数
        StringBuffer urlSb = new StringBuffer();
        if (StringUtil.isEmpty(from)) {
            from = String.valueOf(FROM_QUERY_NUM);
        }
        if (StringUtil.isEmpty(size)) {
            size = String.valueOf(DEFAULT_QUERY_NUM);
        }
        type = DatetimeUtil.getYearToHourTimeStamp(Long.parseLong(start));
    /*    if (index.equals("cdnlog.topurl.count") || index.equals("cdnlog.refer.topn") || index.equals("cdnlog.search.topn") ||
            index.equals("cdnlog.fulldownload") || index.equals("cdnlog.topurl.traffic") || index.equals("cdnlog.topuv.count") ||   
            index.equals("cdnlog.topuv.prv") || index.equals("cdnlog.topdir.count") || index.equals("cdnlog.topdir.traffic") ) {
            type = type.substring(0, 4);
        }*/
        if(!index.equals("cdnlog.badreq.count.hour")){
        	type = type.substring(0, 8);
        }
        urlSb.append(EsRequestBalance.getOneServer()).append("/")
             .append(index).append("/")
             .append(type).append("/")
             .append(action).append("?")
             .append("from=").append(from)
             .append("&size=").append(size);
        
        //检查Query string syntax
        q = getQuerySql();
        if(!StringUtil.isEmpty(q)) {
            urlSb.append("&q=").append(q);
            if (!StringUtil.isEmpty(timeRangStr)) {
                urlSb.append(" AND ").append(timeRangStr);
            }
        } else {
            if (!StringUtil.isEmpty(timeRangStr)) {
                urlSb.append("&q=").append(timeRangStr);
            }
        }
        
        if (!StringUtil.isEmpty(sort)) {
            urlSb.append("&sort=" + sort);
        }
        if (!StringUtil.isEmpty(pretty) && pretty.equals("true")) {
            urlSb.append("&pretty=true");
        }
        
        String url = urlSb.toString().replaceAll(" ", "%20");
        return url;
	}

    private RequestVO getJsonGroupAgg() {
        RequestVO result = new RequestVO();
        //替换聚合的模板值
        long endLong = Long.parseLong(end);
        aggsJson = aggsJson.replace("$start", start);
        aggsJson = aggsJson.replace("$end", String.valueOf(endLong + 1));
        
        if (group.equals("status")) {
            aggsJson = aggsJson.replace("$group", "\"status\"");
        } else if (group.equals("isp")) {
            aggsJson = aggsJson.replace("$group", "\"isp\"");
        } else if (group.equals("prv")) {
            aggsJson = aggsJson.replace("$group", "\"prv\"");
        } else if (group.equals("domain")) {
            aggsJson = aggsJson.replace("$group", "\"domain\"");
        } else if (group.equals("time")) {
            aggsJson = aggsJson.replace("$group", "\"time\"");
        }           
        
        if (StringUtil.isEmpty(limit)) {
            aggsJson = aggsJson.replace("$limit", String.valueOf(Integer.MAX_VALUE));
        } else {
            aggsJson = aggsJson.replace("$limit", limit);
        }
        
        aggsJson = aggsJson.replace("$sum_field", "\"" + sumField + "\"");
        
        result = new RequestVO();
        result.setQueryString(aggsJson);
        return result;
    }
	
	private String getTagValue(String tag, String value) {
	    String tagStr = new String();
	    StringBuffer tmpStr = new StringBuffer();
        String[] values = value.split(",");
        tmpStr.append("(");
        for (int i=0; i<values.length; i++) {
            tmpStr.append(tag).append(":" + values[i]).append(" OR ");
        }
        tagStr = tmpStr.substring(0, tmpStr.length() - 4);
        return tagStr + ")";
	}

	/**
	 * 封装各接口的tag私有参数
	 * q=(prv:865719 OR prv:867310) AND (domain:haochi.55tuan.com OR domain:v03.ysg.netease.com)
	 */
	private String getQuerySql() {
	    String resultQueryStr = new String();
	    StringBuffer qsb = new StringBuffer();
	    
        if (tag.domain.equals("all")) {
            if (!StringUtil.isEmpty(tag.userid)) {
                qsb.append(getTagValue("userid", tag.userid));
                qsb.append(" AND ");
            }
        }else{
        	if (!StringUtil.isEmpty(tag.domain)) {
                qsb.append(getTagValue("domain", tag.domain));
                qsb.append(" AND ");
            }
        }
	    
        Map<String,String> map = CacheLoader.getMetricTagMap().get(index);
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getKey().equals("isp")) {
                if (!StringUtil.isEmpty(tag.isp)) {
                    qsb.append(getTagValue("isp", tag.getIsp()));
                    qsb.append(" AND ");
                }
            } else if (entry.getKey().equals("prv")) {
                if (!StringUtil.isEmpty(tag.prv)) {
                    qsb.append(getTagValue("prv", tag.prv));
                    qsb.append(" AND ");
                }
            } else if (entry.getKey().equals("status")) {
                if (!StringUtil.isEmpty(tag.status)) {
                    qsb.append(getTagValue("status", tag.status));
                    qsb.append(" AND ");
                }
            } /*else if (entry.getKey().equals("domain")) {
                if (!StringUtil.isEmpty(tag.domain)) {
                    qsb.append(getTagValue("domain", tag.domain));
                    qsb.append(" AND ");
                }
            }*/ else if (entry.getKey().equals("machine")) {
                if (!StringUtil.isEmpty(tag.machine)) {
                    qsb.append(getTagValue("machine", tag.machine));
                    qsb.append(" AND ");
                }
            } else if (entry.getKey().equals("suffix")) {
                if (!StringUtil.isEmpty(tag.suffix)) {
                    qsb.append(getTagValue("suffix", tag.suffix));
                    qsb.append(" AND ");
                }
            } else if (entry.getKey().equals("channel")) {
                if (!StringUtil.isEmpty(tag.channel)) {
                    qsb.append(getTagValue("channel", tag.channel));
                    qsb.append(" AND ");
                }
            } else if (entry.getKey().equals("rank")) {
                if (!StringUtil.isEmpty(tag.rank)) {
                    qsb.append(getTagValue("rank", tag.rank));
                    qsb.append(" AND ");
                }
            } else if (entry.getKey().equals("interval")) {
                if (!StringUtil.isEmpty(tag.interval)) {
                    qsb.append(getTagValue("interval", tag.interval));
                    qsb.append(" AND ");
                }
            } else if (entry.getKey().equals("hour")) {
                if (!StringUtil.isEmpty(tag.hour)) {
                    qsb.append(getTagValue("hour", tag.hour));
                    qsb.append(" AND ");
                }
            } else if (entry.getKey().equals("ua")) {
                if (!StringUtil.isEmpty(tag.ua)) {
                    qsb.append(getTagValue("ua", tag.ua));
                    qsb.append(" AND ");
                }
            } else if (entry.getKey().equals("ip")) {
                if (!StringUtil.isEmpty(tag.ip)) {
                    qsb.append(getTagValue("ip", tag.ip));
                    qsb.append(" AND ");
                }
            } else if (entry.getKey().equals("dir")) {
                if (!StringUtil.isEmpty(tag.dir)) {
                    qsb.append(getTagValue("dir", tag.dir));
                    qsb.append(" AND ");
                }
            } else if (entry.getKey().equals("url")) {
                if (!StringUtil.isEmpty(tag.url)) {
                    qsb.append(getTagValue("url", tag.url));
                    qsb.append(" AND ");
                }
            } else if (entry.getKey().equals("refer")) {
                if (!StringUtil.isEmpty(tag.refer)) {
                    qsb.append(getTagValue("refer", tag.refer));
                    qsb.append(" AND ");
                }
            }
        }
        if (!StringUtil.isEmpty(qsb.toString())) {
            resultQueryStr = qsb.substring(0, qsb.length() - 5).toString();
        }
	    return resultQueryStr;
	}
	
	private void receiveParams() throws Exception {
	    log.info("client ip:" + request.getRemoteHost() + " GET " + request.getRequestURL().toString() + "?" + request.getQueryString());
        index = request.getParameter("index");
        start = request.getParameter("start");
        end = request.getParameter("end");
        from = request.getParameter("from");
        size = request.getParameter("size");
        group = request.getParameter("group");
        limit = request.getParameter("limit");
        sumField = request.getParameter("sum_field");
        pretty = request.getParameter("pretty");
        sort = request.getParameter("sort");

        tag.isp = request.getParameter("isp");
        tag.prv = request.getParameter("prv");
        tag.status = request.getParameter("status");
        tag.domain = request.getParameter("domain");
        tag.interval = request.getParameter("interval");
        tag.hour = request.getParameter("hour");
        tag.machine = request.getParameter("machine");
        tag.suffix = request.getParameter("suffix");
        tag.url = request.getParameter("url");
        tag.ip = request.getParameter("ip");
        tag.rank = request.getParameter("rank");
        tag.refer = request.getParameter("refer");
        tag.dir = request.getParameter("dir");
        tag.ua = request.getParameter("ua");
        tag.channel = request.getParameter("channel");
        tag.userid = request.getParameter("userid");
	}
	
	/**
	 * @return
        0000|参数校验正确
        0999|其它错误
        1000|该查询条件没有数据
        1001|缺少index参数
        1011|参数index不合法
        1002|start参数和end参数时间不属于同一个自然月
        1003|start参数和end参数时间不属于同一个自然年
        1004|缺少start参数
        1014|参数start的时间戳错误
        1005|缺少end参数
        1015|参数end的时间戳错误
        1006|缺少group参数
        1007|缺少sum_field参数
	 * @throws Exception
	 * @变更记录 Oct 10, 201410:39:10 AM
	 */
	private String checkParam() throws Exception {
        if (StringUtil.isEmpty(index)) {
            return "1001";
        } else if (StringUtil.isEmpty(CacheLoader.getMetricMap().get(index))) {
            return "1011";
        }
        
        if (StringUtil.isEmpty(start)) {
            return "1004";
        } else if (start.length() != 10) {
            return "1014";
        }
        
        if (StringUtil.isEmpty(end)) {
            return "1005";
        } else if (end.length() != 10) {
            return "1015";
        }
        
        String startYearMonth = DatetimeUtil.getYearMonthByTimeStamp(Long.parseLong(start));
        String endYearMonth = DatetimeUtil.getYearMonthByTimeStamp(Long.parseLong(end));
        String startYear = startYearMonth.substring(0, 4);
        String endYear = endYearMonth.substring(0, 4);
        if (index.equals("cdnlog.topurl.count") || index.equals("cdnlog.refer.topn") || index.equals("cdnlog.search.topn") ||
            index.equals("cdnlog.fulldownload") || index.equals("cdnlog.topurl.traffic") || index.equals("cdnlog.topuv.count") ||   
            index.equals("cdnlog.topuv.prv") || index.equals("cdnlog.topdir.count") || index.equals("cdnlog.topdir.traffic") ) {
            if (!startYear.equals(endYear)) {
                return "1003";
            }
        } else {
            if (!startYearMonth.equals(endYearMonth)) {
                return "1002";
            }
        }
        
        if (StringUtil.isEmpty(group) && !StringUtil.isEmpty(sumField)) {
            return "1006";
        } 
        
        if (StringUtil.isEmpty(sumField) && !StringUtil.isEmpty(group)) {
            return "1007";
        }
        
        return "0000";
	}

	public static void main(String[] args) throws Exception {
		BigdataQueryAction action = new BigdataQueryAction();
		System.out.println(action.execute());
	}
}
