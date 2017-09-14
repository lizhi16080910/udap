<%@ page language="java" import="java.util.*" pageEncoding="UTF-8"%>
<%
	String path = request.getContextPath();
	String basePath = request.getScheme() + "://" + request.getServerName() + ":" + request.getServerPort() + path + "/";
%>

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
<head>
 <style type="text/css">
/* GitHub stylesheet for MarkdownPad (http://markdownpad.com) */
/* Author: Nicolas Hery - http://nicolashery.com */
/* Version: b13fe65ca28d2e568c6ed5d7f06581183df8f2ff */
/* Source: https://github.com/nicolahery/markdownpad-github */

/* RESET
=============================================================================*/

html, body, div, span, applet, object, iframe, h1, h2, h3, h4, h5, h6, p, blockquote, pre, a, abbr, acronym, address, big, cite, code, del, dfn, em, img, ins, kbd, q, s, samp, small, strike, strong, sub, sup, tt, var, b, u, i, center, dl, dt, dd, ol, ul, li, fieldset, form, label, legend, table, caption, tbody, tfoot, thead, tr, th, td, article, aside, canvas, details, embed, figure, figcaption, footer, header, hgroup, menu, nav, output, ruby, section, summary, time, mark, audio, video {
  margin: 0;
  padding: 0;
  border: 0;
}

/* BODY
=============================================================================*/

body {
  font-family: Helvetica, arial, freesans, clean, sans-serif;
  font-size: 14px;
  line-height: 1.6;
  color: #333;
  background-color: #fff;
  padding: 20px;
  max-width: 960px;
  margin: 0 auto;
}

body>*:first-child {
  margin-top: 0 !important;
}

body>*:last-child {
  margin-bottom: 0 !important;
}

/* BLOCKS
=============================================================================*/

p, blockquote, ul, ol, dl, table, pre {
  margin: 15px 0;
}

/* HEADERS
=============================================================================*/

h1, h2, h3, h4, h5, h6 {
  margin: 20px 0 10px;
  padding: 0;
  font-weight: bold;
  -webkit-font-smoothing: antialiased;
}

h1 tt, h1 code, h2 tt, h2 code, h3 tt, h3 code, h4 tt, h4 code, h5 tt, h5 code, h6 tt, h6 code {
  font-size: inherit;
}

h1 {
  font-size: 28px;
  color: #000;
}

h2 {
  font-size: 24px;
  border-bottom: 1px solid #ccc;
  color: #000;
}

h3 {
  font-size: 18px;
}

h4 {
  font-size: 16px;
}

h5 {
  font-size: 14px;
}

h6 {
  color: #777;
  font-size: 14px;
}

body>h2:first-child, body>h1:first-child, body>h1:first-child+h2, body>h3:first-child, body>h4:first-child, body>h5:first-child, body>h6:first-child {
  margin-top: 0;
  padding-top: 0;
}

a:first-child h1, a:first-child h2, a:first-child h3, a:first-child h4, a:first-child h5, a:first-child h6 {
  margin-top: 0;
  padding-top: 0;
}

h1+p, h2+p, h3+p, h4+p, h5+p, h6+p {
  margin-top: 10px;
}

/* LINKS
=============================================================================*/

a {
  color: #4183C4;
  text-decoration: none;
}

a:hover {
  text-decoration: underline;
}

/* LISTS
=============================================================================*/

ul, ol {
  padding-left: 30px;
}

ul li > :first-child, 
ol li > :first-child, 
ul li ul:first-of-type, 
ol li ol:first-of-type, 
ul li ol:first-of-type, 
ol li ul:first-of-type {
  margin-top: 0px;
}

ul ul, ul ol, ol ol, ol ul {
  margin-bottom: 0;
}

dl {
  padding: 0;
}

dl dt {
  font-size: 14px;
  font-weight: bold;
  font-style: italic;
  padding: 0;
  margin: 15px 0 5px;
}

dl dt:first-child {
  padding: 0;
}

dl dt>:first-child {
  margin-top: 0px;
}

dl dt>:last-child {
  margin-bottom: 0px;
}

dl dd {
  margin: 0 0 15px;
  padding: 0 15px;
}

dl dd>:first-child {
  margin-top: 0px;
}

dl dd>:last-child {
  margin-bottom: 0px;
}

/* CODE
=============================================================================*/

pre, code, tt {
  font-size: 12px;
  font-family: Consolas, "Liberation Mono", Courier, monospace;
}

code, tt {
  margin: 0 0px;
  padding: 0px 0px;
  white-space: nowrap;
  border: 1px solid #eaeaea;
  background-color: #f8f8f8;
  border-radius: 3px;
}

pre>code {
  margin: 0;
  padding: 0;
  white-space: pre;
  border: none;
  background: transparent;
}

pre {
  background-color: #f8f8f8;
  border: 1px solid #ccc;
  font-size: 13px;
  line-height: 19px;
  overflow: auto;
  padding: 6px 10px;
  border-radius: 3px;
}

pre code, pre tt {
  background-color: transparent;
  border: none;
}

kbd {
    -moz-border-bottom-colors: none;
    -moz-border-left-colors: none;
    -moz-border-right-colors: none;
    -moz-border-top-colors: none;
    background-color: #DDDDDD;
    background-image: linear-gradient(#F1F1F1, #DDDDDD);
    background-repeat: repeat-x;
    border-color: #DDDDDD #CCCCCC #CCCCCC #DDDDDD;
    border-image: none;
    border-radius: 2px 2px 2px 2px;
    border-style: solid;
    border-width: 1px;
    font-family: "Helvetica Neue",Helvetica,Arial,sans-serif;
    line-height: 10px;
    padding: 1px 4px;
}

/* QUOTES
=============================================================================*/

blockquote {
  border-left: 4px solid #DDD;
  padding: 0 15px;
  color: #777;
}

blockquote>:first-child {
  margin-top: 0px;
}

blockquote>:last-child {
  margin-bottom: 0px;
}

/* HORIZONTAL RULES
=============================================================================*/

hr {
  clear: both;
  margin: 15px 0;
  height: 0px;
  overflow: hidden;
  border: none;
  background: transparent;
  border-bottom: 4px solid #ddd;
  padding: 0;
}

/* TABLES
=============================================================================*/

table th {
  font-weight: bold;
}

table th, table td {
  border: 0px solid #ccc;
  padding: 6px 13px;
}

table tr {
  border-top: 1px solid #ccc;
  background-color: #fff;
}

table tr:nth-child(2n) {
  background-color: #f8f8f8;
}

/* IMAGES
=============================================================================*/

img {
  max-width: 100%
}
</style>
 


<script language="javascript">
var flag=5;
function addRow(){
flag = flag+1;

//添加一行

 			var currentRows = document.getElementById("query_table").rows.length; 
            var insertTr = document.getElementById("query_table").insertRow(currentRows-4);
            var insertTd = insertTr.insertCell(0);
            insertTr.innerHTML = " <tr><td><select name='col" + flag + "'><option value='machine'>主机</option><option value='ip'>IP地址</option><option value='isp'>运营商代码</option><option value='prv'>省份代码</option><option value='domain'>域名</option><option value='url'>URL</option><option value='suffix'>URL后缀</option><option value='status'>状态码</option><option value='cs'>流量</option><option value='refer'>referer字段</option><option value='ua'>用户代理</option><option value='es'>数据发送完毕到关闭请求时间(毫秒)</option><option value='hit'>命中状态</option><option value='dd'>文件完整下载标志（完成1，其他0）</option><option value='userid'>用户编号</option><option value='timestmp'>时间戳</option></select>		&nbsp;&nbsp;&nbsp;		条件：		<select name='term" + flag + "'><option value='1'>等于</option><option value='2' >大于</option><option value='3' selected>小于</option><option value='4'>模糊匹配</option><option value='8' selected>统计条数(count)</option><option value='9' >合计(sum)</option><option value='5' >分组(group by)</option><option value='6'>升序-小到大(order by)</option><option value='7'>倒序-大到小(order by desc)</option></select>	 <input type='text' name='text" + flag + "'></td><td> </td></tr>";

}

 //删除行
 function deleteRow(){
  var tableObj = document.getElementById("query_table");
  var lastTrIndex = tableObj.rows.length - 5; //表格最后一行索引
  if(lastTrIndex > 0){
         tableObj.deleteRow(lastTrIndex);
  }
 }

</script>

<base href="<%=basePath%>">

<title>My JSP 'index.jsp' starting page</title>

<!--
	<meta http-equiv="pragma" content="no-cache">
	<meta http-equiv="cache-control" content="no-cache">
	<meta http-equiv="expires" content="0">    
	<meta http-equiv="keywords" content="keyword1,keyword2,keyword3">
	<meta http-equiv="description" content="This is my page">
	<link rel="stylesheet" type="text/css" href="styles.css">
	-->

</head>

<body>
<form action="/udap/cdnlogqueryaction.action" method="action">
		 <table id="query_table">
		 
		
		 
		 <br>
		 
		 	<tr>
				<td colspan="3"><h1>CDNLOG日志查询示例<h1/></td>
				
			</tr>
			
			<tr>
				<td colspan="3">
				统计某个域名在某天中，状态码为404的错误的请求数 和流量总计
			</tr>
			
			
			<tr>
				<td colspan="3"><strong>查询字段：</strong></td>
			</tr>
			
			<tr>
			<td  colspan="3"><input name="cList" type="checkbox" value="machine"/>主机
			<input name="cList" type="checkbox" value="ip"/>IP地址
			<input name="cList" type="checkbox" value="isp"/>运营商代码
			<input name="cList" type="checkbox" value="prv"/>省份代码
			<input name="cList" type="checkbox" value="domain"/>域名
			<input name="cList" type="checkbox" value="url"/>URL
			<input name="cList" type="checkbox" value="suffix"/>URL后缀
			<input name="cList" type="checkbox" value="timestmp"/>时间戳
			</td>
			</tr>
			
			<tr>
			<td  colspan="3">
			<input name="cList" type="checkbox" value="status" checked/>状态码
			<input name="cList" type="checkbox" value="cs" checked/>流量
			<input name="cList" type="checkbox" value="refer"/>referer字段
			<input name="cList" type="checkbox" value="es"/>请求时间
			<input name="cList" type="checkbox" value="hit"/>命中状态
			<input name="cList" type="checkbox" value="dd"/>文件完整下载标志
			<input name="cList" type="checkbox" value="userid"/>用户编号
			</td>
			</tr>
			
			
			<tr>
				<td colspan="3"></td>
			</tr>
			
		 
		 <tr>
				<td colspan="3"> <strong>查询条件：</strong><a href="javascript:addRow()">添加字段</a> &nbsp; <a href='javascript:deleteRow()'>删除字段</></td>
				
			</tr>
		
			<tr>
				<td>
					<select name="col1">
						<option value="machine">主机</option>
						<option value="ip">IP地址</option>
						<option value="isp">运营商代码</option>
						<option value="prv">省份代码</option>
						<option value="domain" selected>域名</option>
						<option value="url">URL</option>
						<option value="suffix">URL后缀</option>
						<option value="status">状态码</option>
						<option value="cs">流量</option>
						<option value="refer">referer字段</option>
						<option value="ua">用户代理</option>
						<option value="es">数据发送完毕到关闭请求时间(毫秒)</option>
						<option value="hit">命中状态</option>
						<option value="dd">文件完整下载标志（完成1，其他0）</option>
						<option value="userid">用户编号</option>
						<option value="timestmp">时间戳</option>
					</select>
					&nbsp;&nbsp;&nbsp;
					条件：
					<select name="term1">
						<option value="1" selected>等于</option>
						<option value="2" >大于</option>
						<option value="3" >小于</option>
						<option value="4">模糊匹配</option>
						<option value="8" >统计条数(count)</option>
						<option value="9" >合计(sum)</option>
						<option value="5">分组(group by)</option>
						<option value="6">升序-小到大(order by)</option>
						<option value="7">倒序-大到小(order by desc)</option>
					</select>
				 <input type="text" name="text1" value="fs.android2.kugou.com"></td>
				<td></td>
			</tr>
			
			<tr>
				<td>
					<select name="col3">
						<option value="machine">主机</option>
						<option value="ip">IP地址</option>
						<option value="isp">运营商代码</option>
						<option value="prv">省份代码</option>
						<option value="domain" >域名</option>
						<option value="url">URL</option>
						<option value="suffix">URL后缀</option>
						<option value="status" selected>状态码</option>
						<option value="cs">流量</option>
						<option value="refer">referer字段</option>
						<option value="ua">用户代理</option>
						<option value="es">数据发送完毕到关闭请求时间(毫秒)</option>
						<option value="hit">命中状态</option>
						<option value="dd">文件完整下载标志（完成1，其他0）</option>
						<option value="userid">用户编号</option>
						<option value="timestmp">时间戳</option>
					</select>
					&nbsp;&nbsp;&nbsp;
					条件：
					<select name="term3">
						<option value="1" selected>等于</option>
						<option value="2" >大于</option>
						<option value="3" >小于</option>
						<option value="4">模糊匹配</option>
						<option value="8" >统计条数(count)</option>
						<option value="9" >合计(sum)</option>
						<option value="5">分组(group by)</option>
						<option value="6">升序-小到大(order by)</option>
						<option value="7">倒序-大到小(order by desc)</option>
					</select>
				 <input type="text" name="text3" value="404"></td>
				<td></td>
			</tr>
			
			
			<tr>
				<td>
					<select name="col2">
						<option value="machine">主机</option>
						<option value="ip" >IP地址</option>
						<option value="isp">运营商代码</option>
						<option value="prv">省份代码</option>
						<option value="domain" >域名</option>
						<option value="url">URL</option>
						<option value="suffix">URL后缀</option>
						<option value="status">状态码</option>
						<option value="cs" selected>流量</option>
						<option value="refer">referer字段</option>
						<option value="ua">用户代理</option>
						<option value="es">数据发送完毕到关闭请求时间(毫秒)</option>
						<option value="hit">命中状态</option>
						<option value="dd">文件完整下载标志（完成1，其他0）</option>
						<option value="userid">用户编号</option>
						<option value="timestmp">时间戳</option>
					</select>
					&nbsp;&nbsp;&nbsp;
					条件：
					<select name="term2">
						<option value="1">等于</option>
						<option value="2" >大于</option>
						<option value="3" selected>小于</option>
						<option value="4">模糊匹配</option>
						<option value="8" selected>统计条数(count)</option>
						<option value="9" selected>合计(sum)</option>
						<option value="5" >分组(group by)</option>
						<option value="6">升序-小到大(order by)</option>
						<option value="7">倒序-大到小(order by desc)</option>
					</select></td>
				
				<td></td>
			</tr>
			
			 
			 
			 <tr>
				<td>
					<select name="col4">
						<option value="machine">主机</option>
						<option value="ip" >IP地址</option>
						<option value="isp">运营商代码</option>
						<option value="prv">省份代码</option>
						<option value="domain" >域名</option>
						<option value="url">URL</option>
						<option value="suffix">URL后缀</option>
						<option value="status" selected>状态码</option>
						<option value="cs" >流量</option>
						<option value="refer">referer字段</option>
						<option value="ua">用户代理</option>
						<option value="es">数据发送完毕到关闭请求时间(毫秒)</option>
						<option value="hit">命中状态</option>
						<option value="dd">文件完整下载标志（完成1，其他0）</option>
						<option value="userid">用户编号</option>
						<option value="timestmp">时间戳</option>
					</select>
					&nbsp;&nbsp;&nbsp;
					条件：
					<select name="term4">
						<option value="1">等于</option>
						<option value="2" >大于</option>
						<option value="3" selected>小于</option>
						<option value="4">模糊匹配</option>
						<option value="8" selected>统计条数(count)</option>
						<option value="9">合计(sum)</option>
						<option value="5" >分组(group by)</option>
						<option value="6">升序-小到大(order by)</option>
						<option value="7">倒序-大到小(order by desc)</option>
					</select></td>
				
				<td></td>
			</tr>
			
			 
			
			
			
			
			
			<tr>
			<td>
			开始时间：<input type="text" name="start" value="1438963200">
			结束时间：<input type="text" name="end" value="1439049599">
			</td>
			</tr>
			
			
			<tr>
				<td colspan="3"></td>
			</tr>
			
		 
			 
			 
			 
			
				 
			<tr>
				<td><input type="radio" name="detail" value="1" checked/>查看明细&nbsp;&nbsp;&nbsp;&nbsp;
				
				显示
					<select name="limit">
						<option value="10">10条</option>
						<option value="20">20条</option>
						<option value="50">50条</option>
						<option value="100">100条</option>
					</select>
					
					&nbsp;&nbsp;
				<!--<input type="radio" name="detail" value="0"/>仅统计总条数-->
				
				
				</td>
			</tr>
			 
			<tr>
				<td colspan="3"><input type="submit" value="查询"></td>
			</tr>
			
			
			
		</table>
	</form>
	
	
	
	<h2>表单查询说明</h2>
				<strong>1、统计某个域名在某天中，状态码为404的错误的请求数 和流量总计</strong>
		
				<pre><code>select count(status),sum(cs) from cdnlog_merge where  1=1  
and month_ = '201508' and day_ = '08'  and timestmp between '20150800000000' and '20150823595900' 
and domain = 'fs.android2.kugou.com' and status = '404' </code></pre>
<p><a href="http://115.231.46.83:8088/udap/query.jsp">点击进入查询示例页面</a></p>

<strong>2、某个域名在某天中，状态码为404的错误在top10的设备</strong>
		
				<pre><code>select machine,count(status) as status from cdnlog_merge where  1=1  
and month_ = '201508' and day_ = '08'  and timestmp between '20150800000000' and '20150823595900' 
and domain = 'fs.android2.kugou.com' and status = '404' group by machine  order by status desc   limit  10</code></pre>
	<p><a href="http://115.231.46.83:8088/udap/query2.jsp">点击进入查询示例页面</a></p>		 
	 
	 
<h2>接口简要说明</h2>
<p>允许用户自由添加、删除需要查询的字段，并对条件进行组合</p>
<p>点击“查询”按钮，地址栏跳转的URL如下：</p>
<pre><code>http://115.231.46.83:8088/udap/cdnlogqueryaction.action?col1=machine&amp;term1=4&amp;text1=ctl-gs-118-180-000&amp;col2=ip&amp;term2=1&amp;text2=710930032&amp;col3=domain&amp;term3=1&amp;text3=p2.pstatp.com&amp;col4=cs&amp;term4=2&amp;text4=10000&amp;col5=cs&amp;term5=3&amp;text5=20000&amp;start=1438963200&amp;end=1439049599&amp;limit=10
</code></pre>

<p>其中，</p>
<pre><code>http://115.231.46.83:8088/udap/cdnlogqueryaction.action
</code></pre>

<p>为查询接口固定地址</p>
<p>链接中 <code>col1=machine&amp;term1=4&amp;text1=ctl-gs-118-180-000</code></p>
<ul>
<li>col1</li>
<li>term1</li>
<li>text1</li>
</ul>
<p>对应上面查询表单中，第一行查询条件中的三个输入框，及其对应的值</p>
<p>链接中 <code>col2=ip&amp;term2=1&amp;text2=710930032</code></p>
<ul>
<li>col2</li>
<li>term2</li>
<li>text2</li>
</ul>
<p>对应上面查询表单中，第二行查询条件中的三个输入框，及其对应的值</p>
<p>后面依次类推，每添加一个查询条件，其后缀的数字需保持一致</p>
<p>具体说明详见：<strong>CDN日志分析_大数据接口手册.doc</strong></p>





</body>
</html>
