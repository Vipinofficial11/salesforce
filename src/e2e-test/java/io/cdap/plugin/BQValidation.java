/*
 * Copyright © 2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.utils.SalesforceClient;
import io.cdap.plugin.utils.enums.SObjects;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.sql.*;

/**
 * BQValidation.
 */
public class BQValidation {

  static List<JsonObject> bigQueryResponse = new ArrayList<>();
  static List<Object> bigQueryRows = new ArrayList<>();
  static Gson gson = new Gson();

  public static void main(String[] args) throws IOException, InterruptedException, SQLException, ParseException {
//    List<Object> l=new ArrayList<>();
//
//    getBigQueryTableData("E2E_SOURCE_7ff9f7fc_bf6c_46d1_a050_1d4525e7b575",l);
//    for(int i=0;i<l.size();i++)
//    {
//      System.out.println(l.get(i));
//    }
    //validateSalesforceToBQRecordValues(SObjects.AUTOMATION_CUSTOM__C.value, "myTable7");
    validateBQToSalesforceRecordValues("E2E_SOURCE_7ff9f7fc_bf6c_46d1_a050_1d4525e7b575",SObjects.AUTOMATION_CUSTOM__C.value);
  }


  public static boolean validateSalesforceToBQRecordValues(String objectName, String targetTable)
    throws SQLException, IOException, InterruptedException, ParseException {
    String Id = "a03Dn000008bkucIAA";
    //big query table="myTable7";

    getBigQueryTableData(targetTable, bigQueryRows);
    for (Object rows : bigQueryRows) {
      JsonObject json = gson.fromJson(String.valueOf(rows), JsonObject.class);
      bigQueryResponse.add(json);
    }
    SalesforceClient.queryObject(Id, objectName);
    return compareSalesforceAndJsonData(SalesforceClient.objectResponseList, bigQueryResponse);

  }
  public static boolean validateBQToSalesforceRecordValues(String targetTable,String objectName)
    throws SQLException, IOException, InterruptedException, ParseException {
    String Id = "a03Dn000008c4D8IAI";
    //big query table="E2E_SOURCE_7ff9f7fc_bf6c_46d1_a050_1d4525e7b575";

    getBigQueryTableData(targetTable, bigQueryRows);
    for (Object rows : bigQueryRows) {
      JsonObject json = gson.fromJson(String.valueOf(rows), JsonObject.class);
      bigQueryResponse.add(json);
    }
    SalesforceClient.queryObject(Id, objectName);
    return compareSalesforceAndJsonData(SalesforceClient.objectResponseList, bigQueryResponse);

  }

  private static void getBigQueryTableData(String table, List<Object> bigQueryRows)
    throws IOException, InterruptedException {

    String projectId = PluginPropertyUtils.pluginProp("projectId");
    String dataset = PluginPropertyUtils.pluginProp("dataset");
    String selectQuery = "SELECT TO_JSON(t) FROM `" + projectId + "." + dataset + "." + table + "` AS t";
    TableResult result = BigQueryClient.getQueryResult(selectQuery);
    result.iterateAll().forEach(value -> bigQueryRows.add(value.get(0).getValue()));
  }

  public static boolean compareSalesforceAndJsonData(List<JsonObject> salesforceData,
                                                     List<JsonObject> bigQueryData)
    throws SQLException, ParseException {
    boolean result = false;
    if (bigQueryData == null) {
      Assert.fail("bigQueryData is null");
      return result;
    }

    int columnCountTarget = 0;
    if (salesforceData.size() > 0) {
      columnCountTarget = salesforceData.get(0).entrySet().size();
    }
    // Get the column count of the first JsonObject in bigQueryData
    int jsonObjectIdx = 0;
    int columnCountSource = 0;
    if (bigQueryData.size() > 0) {
      columnCountSource = bigQueryData.get(jsonObjectIdx).entrySet().size();
    }
    // Compare the number of columns in the source and target
//    Assert.assertEquals("Number of columns in source and target are not equal",
//                        columnCountSource, columnCountTarget);

    BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
    String projectId = PluginPropertyUtils.pluginProp("projectId");
    String dataset = PluginPropertyUtils.pluginProp("dataset");
    String tableName = "E2E_SOURCE_7ff9f7fc_bf6c_46d1_a050_1d4525e7b575";
    // Build the table reference
    TableId tableRef = TableId.of(projectId, dataset, tableName);

    // Get the table schema
    Schema schema = bigQuery.getTable(tableRef).getDefinition().getSchema();

    // Iterate over the fields and print the column name and type
    int currentColumnCount = 1;
    while (currentColumnCount <= columnCountSource) {
      for (Field field : schema.getFields()) {
        String columnName = field.getName();
        String columnType = field.getType().toString();
        String columnTypeName = field.getType().getStandardType().name();
        System.out.println("Column Name: " + columnName);
        System.out.println("Column Type: " + columnType);
        System.out.println("Column TypeName: " + columnTypeName);

        switch (columnType) {

          case "BOOLEAN":
            boolean sfDateString = salesforceData.get(0).get(columnName).getAsBoolean();
            boolean bqDateString = bigQueryData.get(jsonObjectIdx).get(columnName).getAsBoolean();
            Assert.assertEquals(sfDateString, bqDateString);
            break;

          case "FLOAT":
            Double source = salesforceData.get(0).get(columnName).getAsDouble();
            Double target = bigQueryData.get(jsonObjectIdx).get(columnName).getAsDouble();
            Assert.assertTrue(String.format("Different values found for column: %s", columnName),
                              source.compareTo(target) == 0);

            break;

          case "TIMESTAMP":
            OffsetDateTime sourceDateTime = OffsetDateTime.parse(
              salesforceData.get(0).get(columnName).getAsString(),
              DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
            // Format the source timestamp in the desired format
            String formattedSourceTimestamp = sourceDateTime.toString();
            String formattedTargetTimestamp = bigQueryData.get(jsonObjectIdx).get(columnName).getAsString();
            Assert.assertEquals(formattedTargetTimestamp, formattedSourceTimestamp);
            break;

          case "TIME":
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSSX");
            DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("HH:mm:ss");
            // Parse the timestamp
            LocalTime sourceTime = LocalTime.parse(salesforceData.get(jsonObjectIdx).get(columnName).getAsString(),
                                              formatter);
            LocalTime TargetTime = LocalTime.parse(bigQueryData.get(jsonObjectIdx).get(columnName).getAsString(),
                                              formatter2);
            Assert.assertEquals(sourceTime, TargetTime);
            break;

          case "DATE":
            JsonElement jsonElem1 = salesforceData.get(0).get(columnName);
            Date sourceVal = (jsonElem1 != null && !jsonElem1.isJsonNull()) ? Date.valueOf(
              jsonElem1.getAsString()) : null;
            JsonElement jsonElem = bigQueryData.get(jsonObjectIdx).get(columnName);
            Date targetVal = (jsonElem != null && !jsonElem.isJsonNull()) ? Date.valueOf(
              jsonElem.getAsString()) : null;
            Assert.assertEquals("Different values found for column : %s", sourceVal, targetVal);
            break;


          case "STRING":
          default:
            JsonElement jsonElement1 = salesforceData.get(0).get(columnName);
            String sourceValue = (jsonElement1 != null && !jsonElement1.isJsonNull()) ? jsonElement1.getAsString() : null;
            JsonElement jsonElement = bigQueryData.get(jsonObjectIdx).get(columnName);
            String targetValue = (jsonElement != null && !jsonElement.isJsonNull()) ? jsonElement.getAsString() : null;
            Assert.assertEquals(
              String.format("Different  values found for column : %s", columnName),
              String.valueOf(sourceValue), String.valueOf(targetValue));
            break;
        }
        currentColumnCount++;
      }
      jsonObjectIdx++;
    }
    Assert.assertFalse("Number of rows in Source table is greater than the number of rows in Target table",
                       salesforceData.size() > bigQueryData.size());
    return true;
  }
}

