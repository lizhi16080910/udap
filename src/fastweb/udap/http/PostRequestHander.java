package fastweb.udap.http;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketTimeoutException;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.DefaultMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.params.CookiePolicy;

import fastweb.udap.util.FileHelper;
import fastweb.udap.util.PropertiesConfig;


/**
 * 名称: PostRequestHander.java
 * 描述: 请求客户端程序
 * 最近修改时间: Oct 14, 201411:42:18 AM
 * @since Oct 14, 2014
 * @author zhangyi
 */
@SuppressWarnings("deprecation")
public class PostRequestHander {

    public static final Log log = LogFactory.getLog(PostRequestHander.class);
    public static final int CONN_TIMEOUT_TIME = PropertiesConfig.getIntProperty("fetcher.connection_timeout", 5000);
    public static final int SOCK_TIMEOUT_TIME = PropertiesConfig.getIntProperty("fetcher.socket_timeout", 60000);

    public static ResponseVO getContentByHttp(String url, RequestVO vo, boolean isGetContent) {
        UTF8PostMethod postMethod = null;
        try {
            HttpClient httpClient = new HttpClient();
            postMethod = new UTF8PostMethod(url);
//            NameValuePair postJson = new NameValuePair("details", vo.getTagMap().get("details"));
//            postMethod.setRequestBody(new NameValuePair[] { postJson });
            
            if (vo != null && vo.getQueryString() != null) {
                postMethod.setRequestBody(vo.getQueryString());
            }
            
            postMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());
            postMethod.setRequestHeader("User-Agent",
                    "Mozilla/5.0 (X11; U; Linux i686; zh-CN; rv:1.9.2.13) Gecko/20101206 Ubuntu/10.04 (lucid) Firefox/3.6.13");
            postMethod.setRequestHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
            postMethod.setRequestHeader("Accept-Language", "zh-cn,zh;q=0.5");
            postMethod.setRequestHeader("Accept-Charset", "GB2312,utf-8;q=0.7,*;q=0.7");
            postMethod.setRequestHeader("Keep-Alive", "300");
            postMethod.setRequestHeader("Connection", "close");
            postMethod.setRequestHeader("Content-Type", "application/json; charset=utf-8");

            postMethod.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, SOCK_TIMEOUT_TIME);
            postMethod.getParams().setParameter("http.protocol.cookie-policy", CookiePolicy.BROWSER_COMPATIBILITY);

            DefaultMethodRetryHandler retryhandler = new DefaultMethodRetryHandler();
            // retryhandler.setRequestSentRetryEnabled(false);
            retryhandler.setRetryCount(1);
            postMethod.setMethodRetryHandler(retryhandler);

            httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(CONN_TIMEOUT_TIME);
            // solve Chinese charset 1
            httpClient.getParams().setContentCharset("utf-8");

            int statusCode = 0;
            statusCode = httpClient.executeMethod(postMethod);
            if (statusCode == HttpStatus.SC_OK) {
                ResponseVO responseContent = new ResponseVO();
                responseContent.setStatusCode(statusCode);
                responseContent.setUri(postMethod.getURI().toString());
                responseContent.setContentType(postMethod.getResponseHeader("Content-Type").getValue());

                log.info("content-type:" + responseContent.getContentType());

                if (null != responseContent.getContentType() && isGetContent) {

                    // solve charset 1
                    String respXmlDoc = postMethod.getResponseBodyAsString();

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
                    responseContent.setRespContent(respXmlDoc);
                }
                return responseContent;
            } else {
                log.info("Request failed. status:" + postMethod.getStatusLine() + ". url:" + url);
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
            if (null != postMethod) {
                postMethod.abort();
                postMethod.releaseConnection();
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
        int pageSize = 1048576;
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
    public static class UTF8PostMethod extends PostMethod {
        public UTF8PostMethod(String url) {
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
        byte[] jsonBytes = FileHelper.read(new File("D:\\workspaces_myeclipse6.5\\udap1.2\\test_search_sum4.txt"));
        String postJson = new String(jsonBytes);
        System.out.println("---------------------------------postJson------------------------\n" + postJson);
        long startTime = System.currentTimeMillis();
        
        vo.setQueryString(postJson);
//        String urlSearch1 = "http://192.168.100.181:9200/cdnlog.status.url/201411/_search?&size=2&q=(isp=11)&pretty=true";
        String urlSearch1 = "http://115.231.46.109:9200/cdnlog.status.url/201412/_search?&size=1&pretty=true";
//        String urlSearch1 = "http://192.168.100.181:8080/udap1.2/cdnlog/bigdataQuery.action?index=cdnlog.status.url&action=_search&size=2" +
//        		                "&start=1416286800&end=1416290400&q=(isp:0)&pretty=true&group=prv";
        System.out.println(getContentByHttp(urlSearch1, vo, true).getRespContent());
        System.out.println("---------------------------------------------------------");
        System.out.println("spend time:" + (System.currentTimeMillis() - startTime));
    }
}




