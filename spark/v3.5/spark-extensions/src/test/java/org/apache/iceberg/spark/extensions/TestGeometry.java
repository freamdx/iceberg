/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

public class TestGeometry extends ExtensionsTestBase {
  private String target = tableName + "1";
  private String source = tableName + "2";

  @AfterEach
  public void after() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s", target);
    sql("DROP TABLE IF EXISTS %s", source);
  }

  @TestTemplate
  public void testDDL() {
    ddl("orc");
    ddl("parquet");
  }

  @TestTemplate
  public void testDML() {
    dml("orc");
    dml("parquet");
  }

  @TestTemplate
  public void testMerge() {
    merge("orc", "orc");
    merge("parquet", "parquet");
    merge("orc", "parquet");
    merge("parquet", "orc");
  }

  private void ddl(String format) {
    // create
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql(
        "CREATE TABLE IF NOT EXISTS %s"
            + " (id INT NOT NULL, geom geometry, h3 BIGINT)"
            + " USING iceberg PARTITIONED BY (h3)"
            + " TBLPROPERTIES ('write.format.default'='%s')",
        tableName, format);
    StructField field = spark.table(tableName).schema().toSeq().apply(1);
    assertThat(field.dataType().typeName()).isEqualTo("geometry");

    // create as select
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql(
        "CREATE TABLE IF NOT EXISTS %s"
            + " USING iceberg PARTITIONED BY (h3)"
            + " TBLPROPERTIES ('write.format.default'='%s')"
            + " AS SELECT 1 as id, ST_GeomFromText('POINT(1 1)') as geom, 2024 as h3",
        tableName, format);
    field = spark.table(tableName).schema().toSeq().apply(1);
    assertThat(field.dataType().typeName()).isEqualTo("geometry");
  }

  private void dml(String format) {
    // create
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql(
        "CREATE TABLE IF NOT EXISTS %s"
            + " (id INT NOT NULL, geom geometry, h3 BIGINT)"
            + " USING iceberg PARTITIONED BY (h3)"
            + " TBLPROPERTIES ('write.format.default'='%s')",
        tableName, format);

    // insert
    sql("INSERT INTO %s select 1, ST_GeomFromText('POINT(2 2)'), 2024", tableName);
    sql(
        "INSERT INTO %s VALUES "
            + " (2, ST_GeomFromText('POINT(3 3)'), 2025),"
            + " (3, ST_GeomFromText('POINT(4 4)'), 2026),"
            + " (4, ST_GeomFromText('POINT(5 5)'), 2026)",
        tableName);
    assertThat(spark.table(tableName).count()).isEqualTo(4);

    // select
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row("POINT (2 2)"), row("POINT (3 3)")),
        sql(
            "SELECT ST_AsText(geom) from %s"
                + " WHERE ST_Intersects(ST_GeomFromText('LINESTRING(1 1, 3 3)'), geom) order by 1",
            tableName));

    // insert overwrite
    sql("INSERT OVERWRITE %s SELECT 1, ST_GeomFromText('POINT(1 2 3)'), 2026", tableName);
    assertThat(spark.table(tableName).count()).isEqualTo(3);

    // delete
    sql(
        "DELETE FROM %s WHERE ST_Intersects(ST_GeomFromText('LINESTRING(1 1, 5 5)'), geom)",
        tableName);
    assertThat(spark.table(tableName).count()).isEqualTo(1);

    // update
    sql(
        "UPDATE %s SET geom = ST_GeomFromText('POINT(3 2 1)')"
            + " WHERE ST_Intersects(ST_GeomFromText('POINT(1 2 3)'), geom)",
        tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row("POINT Z(3 2 1)")),
        sql("SELECT ST_AsText(geom) from %s limit 1", tableName));
  }

  private void merge(String format, String sourceFormat) {
    sql("DROP TABLE IF EXISTS %s", target);
    sql("DROP TABLE IF EXISTS %s", source);

    // target
    sql(
        "CREATE TABLE IF NOT EXISTS %s"
            + " (id INT NOT NULL, geom geometry, h3 BIGINT)"
            + " USING iceberg PARTITIONED BY (h3)"
            + " TBLPROPERTIES ('write.format.default'='%s')",
        target, format);
    sql(
        "INSERT INTO %s VALUES"
            + " (1, ST_GeomFromText('POINT(1 1)'), 2024),"
            + " (2, ST_GeomFromText('POINT(3 3)'), 2025),"
            + " (3, ST_GeomFromText('POINT(4 4)'), 2026)",
        target);

    // source
    sql(
        "CREATE TABLE IF NOT EXISTS %s"
            + " (id INT NOT NULL, geom geometry, h3 BIGINT)"
            + " USING iceberg PARTITIONED BY (h3)"
            + " TBLPROPERTIES ('write.format.default'='%s')",
        source, sourceFormat);
    sql(
        "INSERT INTO %s VALUES"
            + " (2, ST_GeomFromText('POINT(2 2)'), 2024),"
            + " (3, ST_GeomFromText('POINT(3 3)'), 2025),"
            + " (4, ST_GeomFromText('POINT(5 5)'), 2026),"
            + " (5, ST_GeomFromText('POINT(3 4)'), 2026)",
        source);

    // merge
    sql(
        "MERGE INTO %s AS target USING %s AS source"
            + " ON target.id == source.id"
            + " WHEN MATCHED THEN"
            + "   UPDATE SET geom = source.geom, h3 = source.h3"
            + " WHEN NOT MATCHED THEN"
            + "   INSERT (id, geom, h3) VALUES (source.id, source.geom, source.h3)",
        target, source);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row("POINT (2 2)"), row("POINT (3 3)"), row("POINT (5 5)")),
        sql(
            "SELECT ST_AsText(geom) from %s"
                + " WHERE ST_Intersects(ST_GeomFromText('LINESTRING(2 2, 5 5)'), geom) order by 1",
            target));
  }
}
