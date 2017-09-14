package fastweb.udap.http;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketTimeoutException;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.DefaultMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.params.CookiePolicy;
import fastweb.udap.util.PropertiesConfig;


/**
 * 名称: HttpRequestHander.java
 * 描述: http请求处理类
 * 最近修改时间: Oct 10, 201410:31:56 AM
 * @since Oct 10, 2014
 * @author zhangyi
 */
@SuppressWarnings("deprecation")
public class GetRequestHander {

    public static final Log log = LogFactory.getLog(GetRequestHander.class);
    public static final int CONN_TIMEOUT_TIME = PropertiesConfig.getIntProperty("fetcher.connection_timeout", 10000);
    public static final int SOCK_TIMEOUT_TIME = PropertiesConfig.getIntProperty("fetcher.socket_timeout", 60000);

    public static ResponseVO getContentByHttp(String url, RequestVO vo, boolean isGetContent) {
        UTF8GetMethod getMethod = null;
        try {
            HttpClient httpClient = new HttpClient();
        	getMethod = new UTF8GetMethod(url);

        	getMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());
        	getMethod.setRequestHeader("User-Agent",
                    "Mozilla/5.0 (X11; U; Linux i686; zh-CN; rv:1.9.2.13) Gecko/20101206 Ubuntu/10.04 (lucid) Firefox/3.6.13");
        	getMethod.setRequestHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
        	getMethod.setRequestHeader("Accept-Language", "zh-cn,zh;q=0.5");
        	getMethod.setRequestHeader("Accept-Charset", "GB2312,utf-8;q=0.7,*;q=0.7");
            getMethod.setRequestHeader("Keep-Alive", "300");
        	getMethod.setRequestHeader("Connection", "close");

        	getMethod.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, SOCK_TIMEOUT_TIME);
        	getMethod.getParams().setParameter("http.protocol.cookie-policy", CookiePolicy.BROWSER_COMPATIBILITY);

            DefaultMethodRetryHandler retryhandler = new DefaultMethodRetryHandler();
            // retryhandler.setRequestSentRetryEnabled(false);
            retryhandler.setRetryCount(1);
            getMethod.setMethodRetryHandler(retryhandler);

            httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(CONN_TIMEOUT_TIME);
            // solve Chinese charset 1
            httpClient.getParams().setContentCharset("utf-8");

            int statusCode = 0;
            statusCode = httpClient.executeMethod(getMethod);
            if (statusCode == HttpStatus.SC_OK) {
                ResponseVO responseVo = new ResponseVO();
                responseVo.setStatusCode(statusCode);
                responseVo.setUri(getMethod.getURI().toString());
                responseVo.setContentType(getMethod.getResponseHeader("Content-Type").getValue());

                log.info("HttpStatus=200,response content-type:" + responseVo.getContentType());

                if (null != responseVo.getContentType() && isGetContent) {
                    // solve charset 1
                    String respContent = getMethod.getResponseBodyAsString();

                    // String charSet = getMethod.getResponseCharSet();
                    // log.info("charSet=" + charSet);
                    // System.out.println("charSet=" + charSet);

                    /*
                     * solve Chinese charset 2 byte[] responseBody =
                     * getMethod.getResponseBody(); String testContent = new
                     * String(responseBody, "utf-8");
                     * System.out.println(testContent);
                     * responseContent.setHtmlContent(testContent); return
                     * responseContent;
                     */

                    /*
                     * solve Chinese charset 3 InputStream ins =
                     * getMethod.getResponseBodyAsStream(); //construct file
                     * stream and appoint charset BufferedReader br = new
                     * BufferedReader(new InputStreamReader(ins,"utf-8"));
                     * StringBuffer sbf = new StringBuffer(); String line =
                     * null; while ((line = br.readLine()) != null) {
                     * sbf.append(line); } //revoke resource br.close();
                     * System.out.println(sbf.toString()); String htmlContent =
                     * sbf.toString();
                     */

                    // solve charset and content size
                    // InputStream ins = getMethod.getResponseBodyAsStream();
                    // String htmlContent = getContent(ins, charSet);
                    responseVo.setRespContent(respContent);
                }
                return responseVo;
            } else {
                log.info("Request failed. status:" + getMethod.getStatusLine() + ". url:" + url);
            }
        } catch (IllegalArgumentException e) {
            log.error("Exception: IllegalArgumentException, url contains invalid parameter, url=" + url);
        } catch (HttpException e) {
            log.error("HttpException, url=" + url);
        } catch (IOException e) {
            log.error("IOException, url=" + url, e);
        } catch (Exception e) {
            log.error("Exception, url:" + url, e);
        } finally {
            if (null != getMethod) {
            	getMethod.abort();
            	getMethod.releaseConnection();
            }
        }
        return null;
    }

    @SuppressWarnings("unused")
    private static String getContent(InputStream is, String charset) throws IOException, SocketTimeoutException {
        if (is == null) {
            return "";
        }
        InputStreamReader reader = null;
        int pageSize = PropertiesConfig.getIntProperty("fetcher.download_page_size", 1048576);
        int totalSize = 0;
        try {
            reader = new InputStreamReader(is, charset);
            char[] buf = new char[1024];
            int length = -1;
            StringBuilder builder = new StringBuilder();
            while ((length = reader.read(buf, 0, 1024)) > 0) {
                // System.out.println("length=" + length);
                totalSize += length;
                if (totalSize > pageSize) {
                    return "";
                }
                builder.append(buf, 0, length);
            }
            System.out.println("totalSize=" + totalSize);

            String content = builder.toString();
            int index = content.indexOf("<");
            if (index > -1) {
                content = content.substring(content.indexOf("<"));
            }

            return content;
        } finally {
            if (null != reader) {
                reader.close();
            }
            if (null != is) {
                is.close();
            }
        }
    }

    // Inner class for UTF-8 support
    public static class UTF8GetMethod extends GetMethod {
        public UTF8GetMethod(String url) {
            super(url);
        }

        @Override
        public String getRequestCharSet() {
            // return super.getRequestCharSet();
            return "utf-8";
        }
    }

    public static void main(String[] args) throws IOException {
        RequestVO vo = new RequestVO();
        
//        String url = "http://192.168.100.180:4242/api/query?m=sum:cdnlog.traffic{prv=*,isp=11,domain=*,machine=*}&start=1414008000&end=1414008001";  //[]
        String url = "http://www.sina.com.cn";
//        String url = "http://192.168.100.180:4242/api/query?m=sum:cdnlog.traffic{prv=*,isp=11,domain=*,machine=*}&start=1414008000&end=1414008001";   //有数据
//        String url = "http://192.168.100.180:4242/api/query?m=sum:cdnlog.traffic{prv=*,isp=11,domain=*,machine=*}&start=1414015200&end=1414015201";
        System.out.println(getContentByHttp(url, vo, true).getRespContent());
    }
}
