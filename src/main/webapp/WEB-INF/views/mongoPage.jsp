<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Insert title here</title>
</head>
<body>
This is mongo page


</br>
<form action="/dbcompare/mongo/insertRecordPage" method="post">

<input type="submit" value="Insert Record"></input>

</form>
</br>
</br>
<form action="/dbcompare/mongo/searchRecordPage" method="post">

<input type="submit" value="Search Record"></input>

</form>
</br>
</br>
<form action="/dbcompare/mongo/createTablePage" method="post">

<input type="submit" value="Create Table"></input>

</form>

</br>
</br>
<form action="/dbcompare/mongo/importCSVPage" method="post">

<input type="submit" value="Import/Download CSV"></input>

</form>


</br>
</br>



<form action="/dbcompare/home" method="get">

<input type="submit" value="Home Page"></input>

</form>
</br>

</body>
</html>