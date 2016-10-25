package com.databricks.spark.csv;

import java.io.File;
import java.util.HashMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;

public class JavaCsvSuite {
  private transient SQLContext sqlContext;
  private int numCars = 3;

  String carsFile = "src/test/resources/cars.csv";

  private String tempDir = "target/test/csvData/";

  @Before
  public void setUp() {
      SparkSession sparkSession = SparkSession.builder()
              .master("local[2]")
              .appName("JavaCsvSuite")
              .getOrCreate();

      sqlContext = sparkSession.sqlContext();
  }

  @After
  public void tearDown() {
    sqlContext.sparkContext().stop();
    sqlContext = null;
  }

  @Test
  public void testCsvParser() {
    Dataset<Row> df = (new CsvParser()).withUseHeader(true).csvFile(sqlContext, carsFile);
    int result = ((Row[])df.select("model").collect()).length;
    Assert.assertEquals(result, numCars);
  }

  @Test
  public void testLoad() {
    HashMap<String, String> options = new HashMap<String, String>();
    options.put("header", "true");
    options.put("path", carsFile);

    Dataset<Row> df = sqlContext.load("com.databricks.spark.csv", options);
    int result = ((Row[])df.select("year").collect()).length;
    Assert.assertEquals(result, numCars);
  }

  @Test
  public void testSave() {
    Dataset<Row> df = (new CsvParser()).withUseHeader(true).csvFile(sqlContext, carsFile);
    TestUtils.deleteRecursively(new File(tempDir));
    df.select("year", "model").write().mode(SaveMode.Overwrite).save(tempDir + "/com.databricks.spark.csv");

    Dataset<Row> newDf = (new CsvParser()).csvFile(sqlContext, tempDir);
    int result = ((Row[])newDf.select("C1").collect()).length;
    Assert.assertEquals(result, numCars);

  }
}
