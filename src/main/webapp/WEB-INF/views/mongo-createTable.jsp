<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Insert title here</title>
</head>
<body>
create Table


<SCRIPT lang="javascript">

var i=0;

function add(type){
	var element=document.createElement("input");
	
	element.setAttribute("id",'id'+i);
	element.setAttribute("name","type"+i);
	element.setAttribute("type",type);
	
	var currentDoc= document.getElementById("targetArea");
	
	currentDoc.appendChild(element);
	i++;
	
}



</SCRIPT>


<form id="tableData" action="/dbcompare/mongo/createTable"  method="post">

table name : <input type="text" name="tableName" >
<input type="button" value="Add Column Qualifier" onclick="add('text')"></input>
</br>
<span id ="targetArea" >Qualifiers: </span>
</br>
<input type="submit" value="submit" ></input>

</form>

</br>
</br>
</br>
</br>
</br>
</br>
</br>

<form action="/dbcompare/mongoPage" method="get">

<input type="submit" value="Mongo Home"></input>
</form>


<form action="/dbcompare/home" method="get">

<input type="submit" value="Home Page"></input>
</form>


</body>
</html>