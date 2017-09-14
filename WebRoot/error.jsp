<%@ page language="java" import="java.util.*" pageEncoding="UTF-8" errorPage="true"%>
<%
String path = request.getContextPath();
String basePath = request.getScheme()+"://"+request.getServerName()+":"+request.getServerPort()+path+"/";
%>

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
  <head>
    <title>处理异常！</title>

  </head>
  
  <body>
  	<br><br>
  	<h3 align="center">${error}</h3>
  	<center>请检查url是否有错</center>
  	<br><br>
    <div align="center"><a href="javascript:history.go(-1)">返回</a></div>
  </body>
</html>
