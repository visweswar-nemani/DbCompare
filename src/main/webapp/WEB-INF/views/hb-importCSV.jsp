<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Insert title here</title>
</head>
<body>
Import Data in Hbase through CSV file


<form action="/dbcompare/hbase/importCSV" method="post" enctype="multipart/form-data">


<input type="file" name="fileCSV" >
<input type="submit"   value="upload">

</form>
</br>
</br>
</br>

<form action="/dbcompare/hbase/downloadCSV" method="post" >

<input type="submit"   value="Download">
</form>


</br>
</br>
</br>
</br>
</br>
</br>
</br>








<form action="/dbcompare/hbasePage" method="get">

<input type="submit" value="Hbase Home"></input>
</form>


<form action="/dbcompare/home" method="get">

<input type="submit" value="Home Page"></input>
</form>




</body>
</html>