import java.io.{BufferedReader, File, FileReader}
import java.sql.{Connection, DriverManager, PreparedStatement, Statement}
import java.sql.ResultSet


class sql_function {
  var conn: Connection = null
  var conn1: Connection = null
  var stmt: Statement = null
  var stmt1: Statement = null
  var prep: PreparedStatement = null
  Class.forName("org.sqlite.JDBC")

  val DbPath=new File("").getAbsoluteFile
  val url = "jdbc:sqlite:"+DbPath+"\\training.db"
  val url1 = "jdbc:sqlite:"+DbPath+"\\new.db"
  conn = DriverManager.getConnection(url)
  conn1 = DriverManager.getConnection(url1)
  stmt = conn.createStatement
  stmt1=conn1.createStatement

  def Stu: Any = {



    val currentDirectory = new java.io.File("src/main/Input_csv/").getAbsoluteFile

    stmt = conn.createStatement
    var a = ""


    val filename = currentDirectory+"\\student.csv"

    val reader = new BufferedReader(new FileReader(filename))
    val symbol = "student"
    var aLine = new Array[String](10)
    var lastSymbol = ""
    var count=0


    a = reader.readLine()


    while (a != null) {

      aLine = a.split(",")

      var   a1 = reader.readLine()

      if (a != null) {

        if (!symbol.equals(lastSymbol)) {
          try {
            val rs = stmt.executeQuery("select name from sqlite_master where name='" + symbol + "';")
            if (rs.next()) {
              stmt.executeUpdate("drop table if exists '" + symbol + "';")
              stmt.executeUpdate("create table '" + symbol + "'(stu_id int primary key ,name);")
            }
          }
          catch {
            case sqle: java.sql.SQLException =>
              println(sqle)

          }
          lastSymbol = symbol
        }
        val prep = conn.prepareStatement("insert into '" + symbol + "' values (?,?);")
        prep.setString(1, aLine(0)) //symbol
        prep.setString(2, aLine(1)) //date
        //println(aLine(0))
        count=count+1
        prep.addBatch()
        prep.executeBatch()
      }
      a=a1
    }
    reader.close()

     return count

  }
  def School: Any = {



    val currentDirectory = new java.io.File("src/main/Input_csv/").getAbsoluteFile


    conn = DriverManager.getConnection(url)
    stmt = conn.createStatement
    var a = ""


    val filename = currentDirectory+"\\School.csv"

    val reader = new BufferedReader(new FileReader(filename))
    val symbol = "School"
    var aLine = new Array[String](10)
    var lastSymbol = ""
    var count=0
    a = reader.readLine()
    while (a != null) {

      aLine = a.split(",")

       var   a1 = reader.readLine()

      if (a != null) {

        if (!symbol.equals(lastSymbol)) {
          try {
            val rs = stmt.executeQuery("select name from sqlite_master where name='" + symbol + "';")
            if (rs.next()) {
              stmt.executeUpdate("drop table if exists '" + symbol + "';")
              stmt.executeUpdate("create table '" + symbol + "'(sid int primary key ,s_name varchar(20));")
            }
          }
          catch {
            case sqle: java.sql.SQLException =>
              println(sqle)

          }
          lastSymbol = symbol
        }
        val prep = conn.prepareStatement("insert into '" + symbol + "' values (?,?);")
        prep.setString(1, aLine(0)) //symbol
        prep.setString(2, aLine(1)) //date
        //println(aLine(0))
        count=count+1
        prep.addBatch()
        prep.executeBatch()


      }
      a=a1
    }
    reader.close()

    return count


  }
  def mark: Any = {



    val currentDirectory = new java.io.File("src/main/Input_csv/").getAbsoluteFile

    stmt = conn.createStatement

    var a = ""

    val filename = currentDirectory+"\\Mark.csv"

    val reader = new BufferedReader(new FileReader(filename))
    val symbol = "Mark"
    var aLine = new Array[String](10)
    var lastSymbol = ""
    var count=0


    a = reader.readLine()


    while (a != null) {

      aLine = a.split(",")

      var a1 = reader.readLine()

      if (a != null) {
        if (!symbol.equals(lastSymbol)) {
          try {
            val rs = stmt.executeQuery("select name from sqlite_master where name='" + symbol + "';")
            if (rs.next()) {
              stmt.executeUpdate("drop table if exists '" + symbol + "';")
              stmt.executeUpdate("create table '" + symbol + "'(sid int REFERENCES School(sid),stu_id int REFERENCES Student(stu_id) ,marks int );")
            }
          }
          catch {
            case sqle: java.sql.SQLException =>
              println(sqle)

          }
          lastSymbol = symbol
        }
        val prep = conn.prepareStatement("insert into '" + symbol + "' values (?,?,?);")
        prep.setString(1, aLine(0)) //symbol
        prep.setString(2, aLine(1)) //dat
        prep.setString(3, aLine(2))
        //println(aLine(0))
         count=count+1
        prep.addBatch()
        prep.executeBatch()


      }
      a=a1
    }
    reader.close()

    return count
  }
  def Stu_details: Any = {



    val currentDirectory = new java.io.File("src/main/Input_csv/").getAbsoluteFile



    stmt = conn.createStatement
    var a = ""

    val filename =currentDirectory+"\\Stu_details.csv"

    val reader = new BufferedReader(new FileReader(filename))
    val symbol = "Stu_details"
    var aLine = new Array[String](10)
    var lastSymbol = ""
    //aLine=" "
    var count=0

    a = reader.readLine()


    while (a != null) {

      aLine = a.split(",")

      var a1 = reader.readLine()

      if (a != null) {
        if (!symbol.equals(lastSymbol)) {
          try {
            val rs = stmt.executeQuery("select name from sqlite_master where name='" + symbol + "';")
            if (rs.next()) {
              stmt.executeUpdate("drop table if exists '" + symbol + "';")
              stmt.executeUpdate("create table '" + symbol + "'(city varchar(20), email varchar(20), stu_id int references Student(stu_id));")
            }
          }
          catch {
            case sqle: java.sql.SQLException =>
              println(sqle)

          }
          lastSymbol = symbol
        }
        val prep = conn.prepareStatement("insert into '" + symbol + "' values (?,?,?);")
        prep.setString(1, aLine(0)) //symbol
        prep.setString(2, aLine(1)) //date
        prep.setString(3, aLine(2)) //date
        count=count+1

        //println(aLine(0))

        prep.addBatch()
        prep.executeBatch()


      }
    a=a1
    }
    reader.close()

    count


  }
  def innerJoin: Any = {

    stmt = conn.createStatement
    var t_name="Student_details_join"

    val statement1 = conn1.createStatement(ResultSet.TYPE_FORWARD_ONLY , ResultSet.CONCUR_READ_ONLY)
    val stmt1=statement1.executeUpdate("drop table if exists '"+t_name+"'")

    val create="create table '"+t_name+"'(name varchar(20), city varchar(20))"
    val pstmt1 = conn1.prepareStatement(create)
    pstmt1.executeUpdate()
    val sql = "INSERT INTO '"+t_name+"'(" + "name," + "city)" + "VALUES(?,?)"

    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY , ResultSet.CONCUR_READ_ONLY)

    val pstmt = conn1.prepareStatement(sql)
    var symbol = "stu_join"
    val query = "select s.name ,d.city from Student s inner join Stu_details d on s.stu_id=d.stu_id"
    val resultSet6 = stmt.executeQuery(query)
    println("--InnerJion-- ")
    println("Stu_Name" + " | " + "city")


    while (resultSet6.next()) {
      //val host = resultSet1.getString("name")
      val name = resultSet6.getString("name")
      val city = resultSet6.getString("city")

      println(name + " | " + city)
      pstmt.setString(1, name)
      pstmt.setString(2, city)

      pstmt.executeUpdate

    }

  }

  def maximum: Any = {

    stmt = conn.createStatement

    val resultSet1 = stmt.executeQuery("select max(marks) as Max from Mark")
    var max=0
    while (resultSet1.next()) {
      //val host = resultSet1.getString("name")
       max = resultSet1.getInt("Max")}
     var symbol="Max"

      println("Maximum Mark " + max)
      val rs = stmt1.executeQuery("select name from sqlite_master where name='" + symbol + "';")

      if (rs.next()) {
        stmt1.executeUpdate("drop table if exists '" + symbol + "';")
        stmt1.executeUpdate("create table '" + symbol + "'(max int);")


        val prep = conn1.prepareStatement("insert into '" + symbol + "' values (?);")
        prep.setInt(1, max)


        //println(aLine(0))

        prep.addBatch()
        prep.executeBatch()

      }






  }
  def min: Any = {

    val resultSet2 = stmt.executeQuery("SELECT  min(marks)as min FROM Mark")

    while (resultSet2.next()) {
   var symbol="minimum"
      val min = resultSet2.getInt("min")
      println("minimum mark " + min)
      val rs = stmt1.executeQuery("select name from sqlite_master where name='" + symbol + "';")

      if (rs.next()) {
        stmt1.executeUpdate("drop table if exists '" + symbol + "';")
        stmt1.executeUpdate("create table '" + symbol + "'(max int);")


        val prep = conn1.prepareStatement("insert into '" + symbol + "' values (?);")
        prep.setInt(1, min)


        //println(aLine(0))

        prep.addBatch()
        prep.executeBatch()

      }

    }


  }
  def sum: Any = {

    stmt = conn.createStatement
    val resultSet4 = stmt.executeQuery("select sum(marks) as sum from Mark")
    var symbol="Sum"
    while (resultSet4.next()) {
      //val host = resultSet1.getString("name")
      val sum = resultSet4.getInt("sum")
      println("Sum of the mark " + sum)
      val rs = stmt1.executeQuery("select name from sqlite_master where name='" + symbol + "';")
      if (rs.next()) {
        stmt1.executeUpdate("drop table if exists '" + symbol + "';")
        stmt1.executeUpdate("create table '" + symbol + "'(max int);")


        val prep = conn1.prepareStatement("insert into '" + symbol + "' values (?);")
        prep.setInt(1, sum)


        //println(aLine(0))

        prep.addBatch()
        prep.executeBatch()

      }

    }

  }
  def avg: Any = {

    stmt = conn.createStatement
    val resultSet5 = stmt.executeQuery("select avg(marks) as avg from Mark")
    var symbol="Average"
    while (resultSet5.next()) {
      //val host = resultSet1.getString("name")
      val avg = resultSet5.getInt("avg")
      println("Avg mark of each student" + avg)
      val rs = stmt1.executeQuery("select name from sqlite_master where name='" + symbol + "';")
      if (rs.next()) {
        stmt1.executeUpdate("drop table if exists '" + symbol + "';")
        stmt1.executeUpdate("create table '" + symbol + "'(max int);")


        val prep = conn1.prepareStatement("insert into '" + symbol + "' values (?);")
        prep.setInt(1, avg)


        //println(aLine(0))

        prep.addBatch()
        prep.executeBatch()

      }

    }

  }
  def count: Any = {

    val resultSet3 = stmt.executeQuery("select count(marks) as count from Mark")
   var symbol="Count"
    while (resultSet3.next()) {
      //val host = resultSet1.getString("name")
      val count = resultSet3.getInt("count")
      println("Total number of student " + count)
      val rs = stmt1.executeQuery("select name from sqlite_master where name='" + symbol + "';")
      if (rs.next()) {
        stmt1.executeUpdate("drop table if exists '" + symbol + "';")
        stmt1.executeUpdate("create table '" + symbol + "'(max int);")


        val prep = conn1.prepareStatement("insert into '" + symbol + "' values (?);")
        prep.setInt(1, count)


        //println(aLine(0))

        prep.addBatch()
        prep.executeBatch()

      }

    }

  }

  def subQuery: Any = {

    var t_name="SubQuery"

    val statement1 = conn1.createStatement(ResultSet.TYPE_FORWARD_ONLY , ResultSet.CONCUR_READ_ONLY)
    val stmt1=statement1.executeUpdate("drop table if exists '"+t_name+"'")

    val create="create table '"+t_name+"'(Stu_id int, name varchar(20))"
    val pstmt1 = conn1.prepareStatement(create)
    pstmt1.executeUpdate()
    val sql = "INSERT INTO '"+t_name+"'(" + "Stu_id," + "name)" + "VALUES(?,?)"

    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY , ResultSet.CONCUR_READ_ONLY)

    val pstmt = conn1.prepareStatement(sql)

    val query1 = "select * from Student s where s.stu_id in (select stu_id from Stu_details where city = 'Bangalore')"
    val resultSet7 = stmt.executeQuery(query1)
    println("--subQuery-- ")
    println("Id" + " | " + "name")

    while (resultSet7.next()) {
      //val host = resultSet1.getString("name")
      val id = resultSet7.getInt("stu_id")
      val name = resultSet7.getString("name")
      println(id + " | " + name)

      pstmt.setInt(1, id)
      pstmt.setString(2, name)

      pstmt.executeUpdate

    }


  }
  def leftJoin: Any = {

    stmt = conn.createStatement
    var t_name="Student_Mark_join"

    val statement1 = conn1.createStatement(ResultSet.TYPE_FORWARD_ONLY , ResultSet.CONCUR_READ_ONLY)
    val stmt1=statement1.executeUpdate("drop table if exists '"+t_name+"'")

    val create="create table '"+t_name+"'(Stu_id int, name varchar(20),sid int,mark int)"
    val pstmt1 = conn1.prepareStatement(create)
    pstmt1.executeUpdate()
    val sql = "INSERT INTO '"+t_name+"'(" + "Stu_id," + "name,"+"sid,"+"mark)" + "VALUES(?,?,?,?)"

    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY , ResultSet.CONCUR_READ_ONLY)

    val pstmt = conn1.prepareStatement(sql)

    val query2 = "select s.stu_id,s.name,m.sid,m.marks from Student s left join Mark m on s.stu_id=m.stu_id"
    val resultSet8 = stmt.executeQuery(query2)
    println("--left join-- ")
    println("ID" + " | " + "name" + " | " + "Sid" + " | " + "mark")

    while (resultSet8.next()) {
      //val host = resultSet1.getString("name")
      val id = resultSet8.getInt("stu_id")
      val name = resultSet8.getString("name"
      )
      val sid = resultSet8.getInt("sid")
      val mark = resultSet8.getInt("marks")

      println(id + " | " + name + " | " + sid + " | " + mark)
      pstmt.setInt(1, id)
      pstmt.setString(2, name)
      pstmt.setInt(3, sid)
      pstmt.setInt(4, mark)
      pstmt.executeUpdate

      }


  }
  def caseWhen: Any = {

    var t_name="caseWhen"

    val statement1 = conn1.createStatement(ResultSet.TYPE_FORWARD_ONLY , ResultSet.CONCUR_READ_ONLY)
    val stmt1=statement1.executeUpdate("drop table if exists '"+t_name+"'")

    val create="create table '"+t_name+"'(Stu_id int, Result varchar(20))"
    val pstmt1 = conn1.prepareStatement(create)
    pstmt1.executeUpdate()
    val sql = "INSERT INTO '"+t_name+"'(" + "Stu_id," + "Result)" + "VALUES(?,?)"

    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY , ResultSet.CONCUR_READ_ONLY)

    val pstmt = conn1.prepareStatement(sql)

    val caseWhen = "select stu_id,  CASE WHEN marks >40 THEN 'PASS' ELSE 'FAIL' END as RESULT from Mark"

    val resultSet9 = stmt.executeQuery(caseWhen)
    println("--CaseWhen-- ")
    println("Id" + " | " + "Result")

    while (resultSet9.next()) {
      //val host = resultSet1.getString("name")
      val id = resultSet9.getInt("stu_id")
      val RESULT = resultSet9.getString("RESULT")
      println(id + " | " + RESULT)
      pstmt.setInt(1, id)
      pstmt.setString(2, RESULT)
      pstmt.executeUpdate

    }
  }
  def Union: Any = {
    var t_name="Union_mark"

    val statement1 = conn1.createStatement(ResultSet.TYPE_FORWARD_ONLY , ResultSet.CONCUR_READ_ONLY)
    val stmt1=statement1.executeUpdate("drop table if exists '"+t_name+"'")

    val create="create table '"+t_name+"'(Marks int)"
    val pstmt1 = conn1.prepareStatement(create)
    pstmt1.executeUpdate()
    val sql = "INSERT INTO '"+t_name+"'(" + "Marks)" + "VALUES(?)"

    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY , ResultSet.CONCUR_READ_ONLY)

    val pstmt = conn1.prepareStatement(sql)

    val UNION = "select max(marks) as marks from Mark m inner join Stu_details s on m.stu_id=s.stu_id where city = 'Bangalore' union select max(marks) from Mark m inner join Stu_details s on m.stu_id=s.stu_id where city = 'delhi'"

    val resultSe = stmt.executeQuery(UNION)
    println("--UNION-- ")
    println("MARKS")

    while (resultSe.next()) {
      //val host = resultSet1.getString("name")
      val RESULT = resultSe.getInt("marks")
      //val RESULT = resultSe.getString("RESULT")
      println(RESULT)
      pstmt.setInt(1, RESULT)
      pstmt.executeUpdate
    }

  }
  def where: Any = {

    stmt = conn.createStatement
    var t_name="Where_condition"

    val statement1 = conn1.createStatement(ResultSet.TYPE_FORWARD_ONLY , ResultSet.CONCUR_READ_ONLY)
    val stmt1=statement1.executeUpdate("drop table if exists '"+t_name+"'")

    val create="create table '"+t_name+"'(Stu_id int)"
    val pstmt1 = conn1.prepareStatement(create)
    pstmt1.executeUpdate()
    val sql = "INSERT INTO '"+t_name+"'(" + "Stu_id)" + "VALUES(?)"

    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY , ResultSet.CONCUR_READ_ONLY)

    val pstmt = conn1.prepareStatement(sql)
    val  query = "select stu_id from Mark where marks>=35"

    val resultSet = stmt.executeQuery(query)
    println("--where-- ")
    println("Id" )

    while (resultSet.next()) {
      //val host = resultSet1.getString("name")
      val id = resultSet.getInt("stu_id")

      println(id )
      pstmt.setInt(1, id)
      pstmt.executeUpdate
    }

  }
  def orderBy: Any = {
    stmt = conn.createStatement
    var t_name="Orderby"

    val statement1 = conn1.createStatement(ResultSet.TYPE_FORWARD_ONLY , ResultSet.CONCUR_READ_ONLY)
    val stmt1=statement1.executeUpdate("drop table if exists '"+t_name+"'")

    val create="create table '"+t_name+"'(Stu_id int,name varchar(20))"
    val pstmt1 = conn1.prepareStatement(create)
    pstmt1.executeUpdate()
    val sql = "INSERT INTO '"+t_name+"'(" + "Stu_id,"+"name)" + "VALUES(?,?)"

    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY , ResultSet.CONCUR_READ_ONLY)

    val pstmt = conn1.prepareStatement(sql)

    val  query = "select * from Student order by  name"

    val resultSet = stmt.executeQuery(query)
    println("--OrderBy-- ")
    println("ID" + " | " + "name")

    while (resultSet.next()) {
      //val host = resultSet1.getString("name")
      val id = resultSet.getInt("stu_id")
      val name=resultSet.getString("name")

      println(id + " | " + name)
      pstmt.setInt(1, id)
      pstmt.setString(2, name)
      pstmt.executeUpdate

    }

  }


}
