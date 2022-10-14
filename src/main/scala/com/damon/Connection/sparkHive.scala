package com.damon.Connection

import org.apache.spark.sql.SparkSession

object sparkHive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

//    val createTableStr =
//      """
//        |CREATE TABLE emp_1(
//        |     empno INT,     -- 员工表编号
//        |     ename STRING,  -- 员工姓名
//        |     job STRING,    -- 职位类型
//        |     mgr INT,
//        |     hiredate TIMESTAMP,  --雇佣日期
//        |     sal DECIMAL(7,2),  --工资
//        |     comm DECIMAL(7,2),
//        |     deptno INT)   --部门编号
//        |ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
//        |LOCATION '/dataset/emp.txt'
//        |""".stripMargin

    spark.sql("CREATE DATABASE IF NOT EXISTS spark_hive")
    spark.sql("USE spark_hive")
    spark.sql("show tables").show()
    spark.sql("select * from emp").show()
//    spark.sql("LOAD DATA INPATH '/dataset/studenttab10k' OVERWRITE INTO TABLE student")
//    spark.sql("select * from student limit").show()

  }

}
