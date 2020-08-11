<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Insert title here</title>
</head>
<body>
welcome to Home page

</br>

<form action="/dbcompare/mongoPage" method="get">
<button type="submit" name="page" value="mongo">Mongo DB</button>
</form>

</br>
</br>
</br>
</br>


<form action="/dbcompare/hbasePage" method="get">
<button type="submit" name="page" value="hbase">HBASE</button>
</form>
</body>
</html>