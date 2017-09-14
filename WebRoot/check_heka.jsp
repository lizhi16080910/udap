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
            var insertTr = document.getElementById("query_table").insertRow(currentRows-7);
            var insertTd = insertTr.insertCell(0);
            insertTr.innerHTML = " <tr><td><select name='col" + flag + "'>"+
            	"<option value='machine' selected>主机</option>"+
            	"<option value='ip'>ip地址</option>"+
            	"<option value='isp'>运营商代码</option>"+
            	"<option value='prv'>省份代码</option>" +
            	"<option value='domain'>域名</option>" +
            	"<option value='url' >URL</option>"+
            	"<option value='suffix' >域名</option>"+
            	"<option value='timestmp'>时间戳</option>"+
            	"<option value='status'>状态码</option>" +
            	"<option value='cs'>流量</option>" +
            	"<option value='refer'>refer字段</option>"+
            	"<option value='es'>请求时间</option>"+
            	"<option value='hit'>命中状态</option>"+
            	"<option value='dd'>文件完整下载标志</option>"+ 
            	"<option value='userid'>用户编号</option>"+
            	"<option value='platform'>平台</option>"+
            	"<option value='popid'>机房编号</option>"+
            "</select>		&nbsp;&nbsp;&nbsp;		条件：		<select name='term" + flag + "'>"+
            "<option value='1' selected>等于</option>"+
            "<option value='2' >大于</option>"+
            "<option value='3' >小于</option>"+
            "<option value='4'>模糊匹配</option>"+
            "</select>	 <input type='text' name='text" + flag + "'></td><td> </td></tr>";

}

 //删除行
 function deleteRow(){
  var tableObj = document.getElementById("query_table");
  var lastTrIndex = tableObj.rows.length - 8; //表格最后一行索引
  if(lastTrIndex > 0){
         tableObj.deleteRow(lastTrIndex);
  }
 }

</script>
    <script language="javascript" for="window" event="onload">
        if(document.readyState=="complete"){ 
             WdatePicker({el:'d12'}); 
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
    <script language="javascript" type="text/javascript" src="My97DatePicker/WdatePicker.js"></script>
    
	<script language="javascript">
		var http_request;
	
    	function addhref(){           
            //添加一行
 			var currentRows = document.getElementById("href_table").rows.length; 
            var insertTr = document.getElementById("href_table").insertRow(currentRows);
            var insertTd = insertTr.insertCell(0);
            insertTr.innerHTML = " <tr><td colspan=\"3\"><p><a href=\"http://192.168.184.87:8080/udap/data.csv\">下载链接</a><p></td></tr>";
    	}
    	
    	function getResult(){
  	    	if(http_request.readyState == 4){
  	    		if(http_request.status==200){
  	    		
  	    			if(http_request.responseText.indexOf("{\"message\":", 0) != -1){
  	    				alert("error:"+ http_request.responseText)
  	    			}else{
  	    				addhref();
  	    			}
  	    		}else{
  	    			alert("您所请求的页面有误！");
  	    		}
  	    	}
     	}
     
     	function createRequest(){
        	
        	var url = "cdnlogerrorstatussavetofileaction.action?";
        	
        	var cList = document.getElementsByName("clist");
        	for(var i=0;i<cList.length;i++){
        		if(cList[i].checked){
        			url = url + "clist=" + cList[i].value + "&";
        		}       	
        	}       
        
        	var type = document.getElementsByName("type");
        	for(var i=0;i<type.length;i++){
        		if(type[i].checked){
        			url = url + "type=" + type[i].value + "&";
        		}       	
        	}
            
        	for(var i=1;i<10;i++){
       	    	var col = document.getElementsByName("col" + i.toString());
       	    	if(col.length == 0){
       	    		break;
       	    	}       	         	    
       	    	url = url + "col" + i.toString() + "=" + col[0].options[col[0].options.selectedIndex].value + "&";
       	     	 
        		var term = document.getElementsByName("term" + i.toString());
        		url = url + "term" + i.toString() + "=" + term[0].options[term[0].options.selectedIndex].value + "&";
        	
        		var text = document.getElementsByName("text" + i.toString());
        		url = url + "text" + i.toString() + "=" + text[0].value + "&";
        		
        		
        	}
        	
        	var order1 = document.getElementsByName("order1");
        	url = url + "order1=" + order1[0].value + "&";
        
            var order2 = document.getElementsByName("order2");
        	url = url + "order2=" + order2[0].value + "&";
        
        	var startTime = document.getElementsByName("startTime");
        	url = url +"startTime=" +  startTime[0].value+ "&";
        	
        	var endTime = document.getElementsByName("endTime");
        	url = url +"endTime=" +  endTime[0].value+ "&";
        
        	var limit = document.getElementsByName("limit");
        	url = url + "limit="+ limit[0].options[limit[0].options.selectedIndex].value;
            		
			http_request = new XMLHttpRequest();
			http_request.onreadystatechange=getResult;
			http_request.open("GET",url,true);
			http_request.send(null);
		}
     
</script>

<!-- /udap/query3cdnlogsaveasjson.action -->
<form action="hekafwlogcountercheckaction.action" method="action">
         
		 <table id="query_table">
		 
		 <br>
		 	<tr>
				<td colspan="3"><h1>fwlog，heka，计数器三方比对结果查看<h1/></td>
			</tr>
			 
			<tr>
				<td>
			设置查询时间：
					&nbsp;&nbsp;&nbsp;
					<input id="d12" type="text" name="d12" class="Wdate" value="2016-09-20" onFocus="WdatePicker({lang:'zh-cn',maxDate:new Date()})"/>
				    <script type="text/javascript">
                      var date = new Date();
                      var dateString = date.getFullYear()+"-"+(date.getMonth()+1)+"-"+date.getDate();
					  d12.value=dateString;
                  </script>
				</td>
			</tr>
			<tr>
				<td colspan="3">
				    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
				    <input type="submit" value="下载查询结果" ">
				</td>
			</tr>
		</table>
</form>

</body>
</html>
