/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.examples.sql;

// $example on:create_ds$
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
// $example off:create_ds$

// $example on:create_df$
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
// $example off:create_df$

// $example on:init_session$
import org.apache.spark.sql.SparkSession;
// $example off:init_session$

public class JavaSparkSQLExample {
  // $example on:create_ds$
  public static class Person implements Serializable {
    private String name;

    private int age;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getAge() {
      return age;
    }

    public void setAge(int age) {
      this.age = age;
    }
  }
  // $example off:create_ds$

  public static void main(String[] args) {
    // $example on:init_session$
    SparkSession spark = SparkSession
      .builder()
      .appName("Java Spark SQL Example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate();
    // $example off:init_session$

    // $example on:create_df$
    Dataset<Row> df = spark.read().json("examples/src/main/resources/people.json");

    // Displays the content of the DataFrame to stdout
    df.show();
    // age  name
    // null Michael
    // 30   Andy
    // 19   Justin
    // $example off:create_df$

    // $example on:untyped_ops$
    // Print the schema in a tree format
    df.printSchema();
    // root
    // |-- age: long (nullable = true)
    // |-- name: string (nullable = true)

    // Select only the "name" column
    df.select("name").show();
    // name
    // Michael
    // Andy
    // Justin

    // Select everybody, but increment the age by 1
    df.select(df.col("name"), df.col("age").plus(1)).show();
    // name    (age + 1)
    // Michael null
    // Andy    31
    // Justin  20

    // Select people older than 21
    df.filter(df.col("age").gt(21)).show();
    // age name
    // 30  Andy

    // Count people by age
    df.groupBy("age").count().show();
    // age  count
    // null 1
    // 19   1
    // 30   1
    // $example off:untyped_ops$

    // $example on:run_sql$
    // Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("people");

    Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
    sqlDF.show();
    // $example off:run_sql$

    // $example on:create_ds$
    // Encoders for most common types are provided in class Encoders.
    Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), Encoders.INT());
    primitiveDS.map(new MapFunction<Integer, Integer>() {
      @Override
      public Integer call(Integer value) throws Exception {
        return value + 1;
      }
    }, Encoders.INT()); // Returns: [2, 3, 4]

    Person person = new Person();
    person.setName("Andy");
    person.setAge(32);

    // Encoders are also created for Java beans.
    Dataset<Person> javaBeanDS = spark.createDataset(
      Collections.singletonList(person),
      Encoders.bean(Person.class)
    );

    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name.
    String path = "examples/src/main/resources/people.json";
    Dataset<Person> peopleDS = spark.read().json(path).as(Encoders.bean(Person.class));
    // $example off:create_ds$
  }
}
