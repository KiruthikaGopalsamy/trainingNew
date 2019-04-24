import java.io.{BufferedReader, File, FileReader}
import java.sql._
import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

import scala.collection.mutable.{Map => MMap}

class test_function extends FunSuite {
  var conn: Connection = null
  var stmt: Statement = null
  var prep: PreparedStatement = null
  Class.forName("org.sqlite.JDBC")

  val currentDirectory = new java.io.File("src\\test\\expected_csv\\").getAbsoluteFile
  val DbPath = new File("").getAbsoluteFile
  val url = "jdbc:sqlite:" + DbPath + "\\new.db"

  conn = DriverManager.getConnection(url)

  //FUNCTION FOR READING STUDENT CSV FILE
  def stu_csv: Any = {

    Class.forName("org.sqlite.JDBC")
    val currentDirectory = new java.io.File("src\\test\\expected_csv\\").getAbsoluteFile
    val filename = currentDirectory + "\\student.csv"
    val spark = SparkSession.builder()
      .master("local")
      .appName("Word Count")
      .getOrCreate()
    val df_1 = spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").option("header", "false").load(filename)
    //comparing student table AND CSV
    var c1 = df_1.count()
    spark.close()
    c1
  }

  def sch_csv: Any = {

    Class.forName("org.sqlite.JDBC")
    val currentDirectory = new java.io.File("src\\test\\expected_csv\\").getAbsoluteFile
    val filename = currentDirectory + "\\School.csv"
    val spark = SparkSession.builder()
      .master("local")
      .appName("Word Count")
      .getOrCreate()
    val df_1 = spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").option("header", "false").load(filename)
    //comparing student table AND CSV
    var c1 = df_1.count()
    spark.close()
    c1
  }

  def st_details_csv: Any = {

    Class.forName("org.sqlite.JDBC")
    val currentDirectory = new java.io.File("src\\test\\expected_csv\\").getAbsoluteFile
    val filename = currentDirectory + "\\Stu_details.csv"
    val spark = SparkSession.builder()
      .master("local")
      .appName("Word Count")
      .getOrCreate()
    val df_1 = spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").option("header", "false").load(filename)
    //comparing student table AND CSV
    var c1 = df_1.count()
    spark.close()
    c1
  }

  def mark_csv: Any = {

    Class.forName("org.sqlite.JDBC")
    val currentDirectory = new java.io.File("src\\test\\expected_csv\\").getAbsoluteFile
    val filename = currentDirectory + "\\Mark.csv"
    val spark = SparkSession.builder()
      .master("local")
      .appName("Word Count")
      .getOrCreate()
    val df_1 = spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").option("header", "false").load(filename)
    //comparing student table AND CSV
    var c1 = df_1.count()
    spark.close()
    c1
  }

  def max: Any = {



    val currentDirectory = new java.io.File("src\\test\\expected_csv\\").getAbsoluteFile
    val filename = currentDirectory + "\\Expected _values.csv"
    var a = 0
    var symbol = "maximum_expected"
    var i = scala.io.Source.fromFile(filename)
    for (l <- i.getLines()) {
      val cols = l.split("=").map(_.trim)
      val m = MMap(cols(0) -> cols(1).toInt)
      for ((k, v) <- m) {
        if (k == "max") {
          a = v

        }
      }
    }

    val rs = stmt.executeQuery("select name from sqlite_master where name='" + symbol + "';")

    if (rs.next()) {
      stmt.executeUpdate("drop table if exists '" + symbol + "';")
      stmt.executeUpdate("create table '" + symbol + "'(max int);")


      val prep = conn.prepareStatement("insert into '" + symbol + "' values (?);")
      prep.setInt(1, a)


      //println(aLine(0))

      prep.addBatch()
      prep.executeBatch()

    }


  }


  def min: Any = {
    val currentDirectory = new java.io.File("src\\test\\expected_csv\\").getAbsoluteFile
    val filename = currentDirectory + "\\Expected _values.csv"
    var a = 0
    var symbol = "minimum_expected"
    var i = scala.io.Source.fromFile(filename)
    for (l <- i.getLines()) {
      val cols = l.split("=").map(_.trim)
      val m = MMap(cols(0) -> cols(1).toInt)
      for ((k, v) <- m) {
        if (k == "min") {
          a = v

        }
      }
    }

    val rs = stmt.executeQuery("select name from sqlite_master where name='" + symbol + "';")

    if (rs.next()) {
      stmt.executeUpdate("drop table if exists '" + symbol + "';")
      stmt.executeUpdate("create table '" + symbol + "'(min_mark int);")


      val prep = conn.prepareStatement("insert into '" + symbol + "' values (?);")
      prep.setInt(1, a)


      //println(aLine(0))

      prep.addBatch()
      prep.executeBatch()

    }


  }


  def sum: Any = {
    var a=0
    var symbol="sum_expected"
    val currentDirectory = new java.io.File("src\\test\\expected_csv\\").getAbsoluteFile
    val filename = currentDirectory + "\\Expected _values.csv"
    var i = scala.io.Source.fromFile(filename)
    for (l <- i.getLines()) {
      import scala.collection.mutable.{Map => MMap}
      val cols = l.split("=").map(_.trim)
      var states = collection.mutable.Map(cols(0) -> cols(1).toInt)
      val m = MMap(cols(0) -> cols(1).toInt)
      for ((k, v) <- m) {
        if (k == "sum")
          a= v
      }
    }
    val rs = stmt.executeQuery("select name from sqlite_master where name='" + symbol + "';")

    if (rs.next()) {
      stmt.executeUpdate("drop table if exists '" + symbol + "';")
      stmt.executeUpdate("create table '" + symbol + "'(sum_mark int);")


      val prep = conn.prepareStatement("insert into '" + symbol + "' values (?);")
      prep.setInt(1, a)




      prep.addBatch()
      prep.executeBatch()

    }
  }

  def avg: Any = {
    var a=0
    var symbol="Average_Expected"
    val currentDirectory = new java.io.File("src\\test\\expected_csv\\").getAbsoluteFile
    val filename = currentDirectory + "\\Expected _values.csv"
    var i = scala.io.Source.fromFile(filename)
    for (l <- i.getLines()) {
      import scala.collection.mutable.{Map => MMap}
      val cols = l.split("=").map(_.trim)
      var states = collection.mutable.Map(cols(0) -> cols(1).toInt)
      val m = MMap(cols(0) -> cols(1).toInt)
      for ((k, v) <- m) {
        if (k == "avg")
          a=v
      }
    }
    val rs = stmt.executeQuery("select name from sqlite_master where name='" + symbol + "';")

    if (rs.next()) {
      stmt.executeUpdate("drop table if exists '" + symbol + "';")
      stmt.executeUpdate("create table '" + symbol + "'(avg int);")


      val prep = conn.prepareStatement("insert into '" + symbol + "' values (?);")
      prep.setInt(1, a)


      //println(aLine(0))

      prep.addBatch()
      prep.executeBatch()

    }
  }

  def count: Any = {

    val currentDirectory = new java.io.File("src\\test\\expected_csv\\").getAbsoluteFile
    val filename = currentDirectory + "\\Expected _values.csv"
    var a=0
    var symbol="Count_expected"
    var i = scala.io.Source.fromFile(filename)
    for (l <- i.getLines()) {
      import scala.collection.mutable.{Map => MMap}
      val cols = l.split("=").map(_.trim)
      var states = collection.mutable.Map(cols(0) -> cols(1).toInt)
      val m = MMap(cols(0) -> cols(1).toInt)
      for ((k, v) <- m) {
        if (k == "count")
          a= v
      }
    }
    val rs = stmt.executeQuery("select name from sqlite_master where name='" + symbol + "';")

    if (rs.next()) {
      stmt.executeUpdate("drop table if exists '" + symbol + "';")
      stmt.executeUpdate("create table '" + symbol + "'(count int);")


      val prep = conn.prepareStatement("insert into '" + symbol + "' values (?);")
      prep.setInt(1, a)


      //println(aLine(0))

      prep.addBatch()
      prep.executeBatch()

    }
  }

  def innerjoin: Any = {

    stmt = conn.createStatement

    var a = ""

    val filename = currentDirectory + "\\join_expected_output.csv"

    val reader = new BufferedReader(new FileReader(filename))
    val symbol ="Student_details_join_expected"
    var aLine= new scala.Array[String](10)
    var lastSymbol = ""

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
              stmt.executeUpdate("create table '" + symbol + "'(sname varchar(20),city varchar(20) );")
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
        prep.setString(2, aLine(1)) //dat

        //println(aLine(0))
        prep.addBatch()
        prep.executeBatch()


      }
      a = a1
    }
    reader.close()


  }
  def  leftjoin:Any={
    stmt = conn.createStatement

    var a = ""

    val filename = currentDirectory + "\\leftJoin_expected_output.csv"

    val reader = new BufferedReader(new FileReader(filename))
    val symbol ="Student_Mark_join_expected"
    var aLine= new scala.Array[String](10)
    var lastSymbol = ""

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
              stmt.executeUpdate("create table '" + symbol + "'(Stu_id int,name varchar(20),sid int,mark int );")
            }
          }
          catch {
            case sqle: java.sql.SQLException =>
              println(sqle)

          }
          lastSymbol = symbol
        }
        val prep = conn.prepareStatement("insert into '" + symbol + "' values (?,?,?,?);")
        prep.setString(1, aLine(0))
        prep.setString(2, aLine(1))
        prep.setString(3, aLine(2))
        prep.setString(4, aLine(3))
        prep.addBatch()
        prep.executeBatch()


      }
      a = a1
    }
    reader.close()



  }
  def  subQuery:Any={


    var a = ""

    val filename = currentDirectory + "\\Subquery_expected_output.csv"

    val reader = new BufferedReader(new FileReader(filename))
    val symbol ="SubQuery_expected"
    var aLine= new scala.Array[String](10)
    var lastSymbol = ""

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
              stmt.executeUpdate("create table '" + symbol + "'(Stu_id int,name varchar(20));")
            }
          }
          catch {
            case sqle: java.sql.SQLException =>
              println(sqle)

          }
          lastSymbol = symbol
        }
        val prep = conn.prepareStatement("insert into '" + symbol + "' values (?,?);")
        prep.setString(1, aLine(0))
        prep.setString(2, aLine(1))

        prep.addBatch()
        prep.executeBatch()


      }
      a = a1
    }
    reader.close()



  }
  def  caseWhen:Any={


    var a = ""

    val filename = currentDirectory + "\\CaseWhen_expected_output.csv"

    val reader = new BufferedReader(new FileReader(filename))
    val symbol ="CaseWhen_expected"
    var aLine= new scala.Array[String](10)
    var lastSymbol = ""

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
              stmt.executeUpdate("create table '" + symbol + "'(Stu_id int,Result varchar(20));")
            }
          }
          catch {
            case sqle: java.sql.SQLException =>
              println(sqle)

          }
          lastSymbol = symbol
        }
        val prep = conn.prepareStatement("insert into '" + symbol + "' values (?,?);")
        prep.setString(1, aLine(0))
        prep.setString(2, aLine(1))

        prep.addBatch()
        prep.executeBatch()


      }
      a = a1
    }
    reader.close()



  }
  def  where_expected:Any={


    var a = ""

    val filename = currentDirectory + "\\where_expected_output.csv"

    val reader = new BufferedReader(new FileReader(filename))
    val symbol ="where_expected_output"
    var aLine= new scala.Array[String](10)
    var lastSymbol = ""

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
              stmt.executeUpdate("create table '" + symbol + "'(Stu_id int);")
            }
          }
          catch {
            case sqle: java.sql.SQLException =>
              println(sqle)

          }
          lastSymbol = symbol
        }
        val prep = conn.prepareStatement("insert into '" + symbol + "' values (?);")
        prep.setString(1, aLine(0))


        prep.addBatch()
        prep.executeBatch()


      }
      a = a1
    }
    reader.close()



  }
  def  Union_marks:Any={


    var a = ""

    val filename = currentDirectory + "\\Union_of_marks_expected.csv"

    val reader = new BufferedReader(new FileReader(filename))
    val symbol ="Union_of_marks_expected"
    var aLine= new scala.Array[String](10)
    var lastSymbol = ""

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
              stmt.executeUpdate("create table '" + symbol + "'(marks int);")
            }
          }
          catch {
            case sqle: java.sql.SQLException =>
              println(sqle)

          }
          lastSymbol = symbol
        }
        val prep = conn.prepareStatement("insert into '" + symbol + "' values (?);")
        prep.setString(1, aLine(0))


        prep.addBatch()
        prep.executeBatch()


      }
      a = a1
    }
    reader.close()



  }
  def OrderBy: Any = {

    stmt = conn.createStatement

    var a = ""

    val filename = currentDirectory + "\\Orderby_expected_output.csv"

    val reader = new BufferedReader(new FileReader(filename))
    val symbol ="Orderby_expected_output"
    var aLine= new scala.Array[String](10)
    var lastSymbol = ""

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
              stmt.executeUpdate("create table '" + symbol + "'(Stu_id int,name varchar(20) );")
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
        prep.setString(2, aLine(1)) //dat

        //println(aLine(0))
        prep.addBatch()
        prep.executeBatch()


      }
      a = a1
    }
    reader.close()


  }

}










