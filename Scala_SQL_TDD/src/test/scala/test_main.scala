import java.io.File
import java.sql.{Connection, DriverManager, PreparedStatement, Statement}

import org.scalatest.{BeforeAndAfter, FunSuite}

class test_main extends FunSuite with BeforeAndAfter {
  var conn: Connection = null
  var stmt: Statement = null
  Class.forName("org.sqlite.JDBC")
  val DbPath = new File("").getAbsoluteFile
  val url = "jdbc:sqlite:" + DbPath + "\\new.db"
  conn = DriverManager.getConnection(url)
  stmt = conn.createStatement
  val table = new sql_function
  val csv = new test_function


 /* test("Student table ") {
    var st = table.Stu
    var st1 = csv.stu_csv
    assert(st === st1)

  }
  test("school table") {
    var sch = csv.sch_csv
    var sch1 = table.School
    assert(sch1 === sch)
  }
  test("stu_details") {
    var sd = table.Stu_details
    var sd1 = csv.st_details_csv
    assert(sd == sd1)
  }
  test("Mark Table") {
    var mark = table.mark
    var mark1 = csv.mark_csv
    assert(mark == mark1)
  }

  test("Minimum Mark") {

    var csv_values = csv.min
    var table_values = table.min
    assert(csv_values == table_values)
  }
  test("Sum ") {

    var csv_values = csv.sum
    var table_values = table.sum
    assert(csv_values == table_values)
  }
  test("Average ") {

    var csv_values = csv.avg
    var table_values = table.avg
    assert(csv_values == table_values)
  }
  test("count ") {

    var csv_values = csv.count
    var table_values = table.count
    assert(csv_values == table_values)

  }*/
  test("Innerjoin") {
    var s=csv.innerjoin

    val query = "select * from Student_details_join_expected except select * from Student_details_join;"
    val resultSet = stmt.executeQuery(query)

     assert(resultSet.next()==false)
   }
  test("Ieftjoin") {
    var k=csv.leftjoin
    val query = "select * from Student_Mark_join_expected except select * from Student_Mark_join;"
    val resultSet = stmt.executeQuery(query)

    assert(resultSet.next()==false)
  }
  test("SubQuery") {
    var k = csv.subQuery
    val query = "select * from SubQuery_expected except select * from SubQuery;"
    val resultSet = stmt.executeQuery(query)

    assert(resultSet.next()==false)

  }
  test("CaseWhen") {
    var k = csv.caseWhen
    val query = "select * from CaseWhen_expected except select * from caseWhen;"
    val resultSet = stmt.executeQuery(query)

    assert(resultSet.next()==false)


  }
  test("Union") {
    var k = csv.Union_marks
    val query = "select * from Union_of_marks_expected except select * from union_mark"
    val resultSet = stmt.executeQuery(query)

    assert(resultSet.next()==false)
  }
  test("Where_condition") {
    var k = csv.where_expected
    val query = "select * from where_expected_output except select * from where_condition"
    val resultSet = stmt.executeQuery(query)
    assert(resultSet.next()==false)
  }
  test("OrderBy") {
    var k = csv.OrderBy
    val query = "select * from Orderby_expected_output except select * from Orderby"
    val resultSet = stmt.executeQuery(query)
    assert(resultSet.next()==false)
  }
  test("Maximum") {
    var k = csv.max
    val query = "select * from Maximum_expected except select * from Max"
    val resultSet = stmt.executeQuery(query)
    assert(resultSet.next()==false)
  }
  test("minimum") {
   var m=csv.min
   val query = "select * from minimum_expected except select * from minimum"
    val resultSet = stmt.executeQuery(query)
    assert(resultSet.next()==false)

  }
  test("Sum") {
    var k=csv.sum
    val query = "select * from Sum_expected except select * from Sum"
    val resultSet = stmt.executeQuery(query)
    assert(resultSet.next()==false)
  }

  test("Average") {
    var k = csv.avg
    val query = "select * from Average_expected except select * from Average"
    val resultSet = stmt.executeQuery(query)
    assert(resultSet.next()==false)
  }
  test("Count") {
    var k = csv.count
    val query = "select * from Count_expected except select * from Count"
    val resultSet = stmt.executeQuery(query)
    assert(resultSet.next()==false)
  }

}
