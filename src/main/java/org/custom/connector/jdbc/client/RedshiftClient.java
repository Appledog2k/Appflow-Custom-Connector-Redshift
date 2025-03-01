// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package org.custom.connector.jdbc.client;

import com.amazonaws.appflow.custom.connector.model.metadata.DescribeEntityRequest;
import com.amazonaws.appflow.custom.connector.model.metadata.Entity;
import com.amazonaws.appflow.custom.connector.model.metadata.FieldDataType;
import com.amazonaws.appflow.custom.connector.model.metadata.FieldDefinition;
import com.amazonaws.appflow.custom.connector.model.metadata.ImmutableEntity;
import com.amazonaws.appflow.custom.connector.model.metadata.ImmutableFieldDefinition;
import com.amazonaws.appflow.custom.connector.model.metadata.ImmutableReadOperationProperty;
import com.amazonaws.appflow.custom.connector.model.metadata.ImmutableWriteOperationProperty;
import com.amazonaws.appflow.custom.connector.model.metadata.ListEntitiesRequest;
import com.amazonaws.appflow.custom.connector.model.query.QueryDataRequest;
import com.amazonaws.appflow.custom.connector.model.write.WriteDataRequest;
import com.amazonaws.appflow.custom.connector.model.write.WriteOperationType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public final class RedshiftClient implements JDBCClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(RedshiftClient.class);
  final ObjectMapper objectMapper = new ObjectMapper();
  private Connection conn = null;
  private final Map<String, String> credentials;

  public RedshiftClient(final Map<String, String> creds) {
    credentials = creds;
  }

  @Override
  public List<WriteOperationType> getWriteOperations() {
    List<WriteOperationType> writeOperationTypes = new ArrayList<>();
    writeOperationTypes.add(WriteOperationType.UPSERT);
    writeOperationTypes.add(WriteOperationType.UPDATE);
    writeOperationTypes.add(WriteOperationType.INSERT);
    return writeOperationTypes;
  }

  // Done - Function GetEntities
  @Override
  public List<Entity> getEntities(final ListEntitiesRequest request) throws SQLException {
    final List<Entity> records = new ArrayList<Entity>();

    Connection conn = getConnection();

    DatabaseMetaData metaData = conn.getMetaData();
    String[] types = { "TABLE" };
    // Retrieving the columns in the database
    ResultSet tables = metaData.getTables(null, null, "%", types);
    while (tables.next()) {
      records.add(
          ImmutableEntity.builder()
              .entityIdentifier(tables.getString("TABLE_NAME"))
              .description(tables.getString("TABLE_NAME"))
              .label(tables.getString("TABLE_NAME"))
              .hasNestedEntities(false) // A boolean indicating whether the entity has nested or child entities
                                        // associated with it.
              .build());
    }
    conn.close();
    return records;
  }

  // Xem lại phần khóa chính Redshift
  @Override
  public List<FieldDefinition> getFieldDefinitions(final DescribeEntityRequest request) throws SQLException {
    final List<FieldDefinition> fieldDefinitions = new ArrayList<>();
    Connection conn = getConnection();

    Statement st = conn.createStatement();
    String sql = String.format("SELECT * FROM pg_table_def WHERE tablename = '%s'", request.entityIdentifier());
    ResultSet rs = st.executeQuery(sql);

    while (rs.next()) {
      fieldDefinitions.add(ImmutableFieldDefinition.builder()
          .fieldName(rs.getString(3))
          .dataType(mapFieldType(rs.getString(4)))
          .dataTypeLabel(rs.getString(3))
          .label(rs.getString(3))
          .readProperties(ImmutableReadOperationProperty.builder()
              .isQueryable(true)
              .isRetrievable(true)
              .build())
          .writeProperties(ImmutableWriteOperationProperty.builder()
              .isNullable(true)
              .isUpdatable(true)
              .isCreatable(true)
              .supportedWriteOperations(getWriteOperations())
              .build())
          .build());
    }
    rs.close();
    conn.close();
    return fieldDefinitions;
  }

  // Done - Function Get Connection
  @Override
  public Connection getConnection() {
    try {
      if (conn != null && conn.isValid(0)) {
        return conn;
      }
    } catch (SQLException e) {
      // Do nothing for now.
    }

    try {
      String uri = String.format(
          "jdbc:%s://%s:%s/%s?user=%s&password=%s",
          credentials.get("driver"),
          credentials.get("hostname"),
          credentials.get("port"),
          credentials.get("database"),
          credentials.get("username"),
          credentials.get("password"));

      conn = DriverManager.getConnection(uri);
    } catch (SQLException ex) {
      LOGGER.error("Develop DFT Log Connection");
      // handle any errors
      LOGGER.error("SQLException: " + ex.getMessage());
      LOGGER.error("SQLState: " + ex.getSQLState());
      LOGGER.error("VendorError: " + ex.getErrorCode());
    }
    return conn;
  }

  // Xem lại phần các trường này
  private FieldDataType mapFieldType(final String redshiftType) {
    String[] temp = redshiftType.split("\\(");
    String mtype = temp[0].toUpperCase();
    switch (mtype) {
      case "SMALLINT":
      case "BIGINT":
      case "DECIMAL":
      case "NUMERIC":
      case "REAL":
      case "DOUBLE PRECISION":
      case "FLOAT8":
      case "FLOAT4":
      case "INTEGER":
        return FieldDataType.Integer;
      case "INT":
        return FieldDataType.Integer;
      case "INT2":
      case "INT4":
      case "CHAR":
      case "VARCHAR":
      case "CHARACTER":
      case "NCHAR":
      case "CHARACTER VARYING":
      case "NVARCHAR":
      case "BPCHAR":
      case "TEXT":
        return FieldDataType.String;
      case "DATE":
        return FieldDataType.Date;
      case "TIME":
      case "TIMETZ":
      case "TIMESTAMP":
      case "TIMESTAMPTZ":
      case "INTERVAL YEAR TO MONTH":
      case "INTERVAL DAY TO SECOND":
      case "BOOLEAN":
        return FieldDataType.Boolean;
      case "HLLSKETCH":
        return FieldDataType.String;
      case "SUPER":
      case "VARBYTE":
      case "GEOMETRY":
        return FieldDataType.String;
      case "GEOGRAPHY":
        return FieldDataType.String;
      default:
        return FieldDataType.String;
    }
  }

  // Done - Function GetTotalData
  @Override
  public long getTotalData(final QueryDataRequest request) {
    try (Connection conn = getConnection()) {
      Statement st = conn.createStatement();

      String sql = String.format("SELECT COUNT(*) as cnt FROM %s", request.entityIdentifier());
      if (request.filterExpression() != null) {
        sql = sql + String.format(" WHERE %s", request.filterExpression());
      }

      ResultSet rs = st.executeQuery(sql);
      rs.next();
      long count = rs.getLong("cnt");
      st.close();
      conn.close();
      return count;
    } catch (SQLException ex) {
      LOGGER.error("SQLException information - DFT Develop Log");
      while (ex != null) {
        LOGGER.error("DFT Develop Log msg: " + ex.getMessage());
        ex = ex.getNextException();
      }
      throw new RuntimeException("Error - DFT Develop Log");
    }
  }

  // Xem lại điều kiện lọc lấy dữ liệu
  @Override
  public List<String> queryData(final QueryDataRequest request) {
    List<String> records = new ArrayList<String>();

    try (Connection conn = getConnection()) {

      Statement st = conn.createStatement();

      List<String> fieldNamesList = request.selectedFieldNames();
      String[] fieldNamesArray = fieldNamesList.toArray(new String[0]);
      String output = Arrays.stream(fieldNamesArray)
          .map(fieldName -> "\"" + fieldName + "\"")
          .collect(Collectors.joining(","));

      String sql = String.format(
          "SELECT %s FROM %s", output, request.entityIdentifier());

      if (request.filterExpression() != null) {
        sql = sql + String.format(" WHERE %s", request.filterExpression());
      }

      if (request.maxResults() != null) {
        int nextToken = 0;
        if (request.nextToken() != null) {
          nextToken = Integer.parseInt(request.nextToken());
        }
        sql = sql + String.format(" OFFSET %s LIMIT %s", nextToken, request.maxResults());
      }

      ResultSet rs = st.executeQuery(sql);
      Map<String, String> rows = new HashMap<>();

      while (rs.next()) {
        for (int i = 0; i < request.selectedFieldNames().size(); i++) {
          rows.put(request.selectedFieldNames().get(i), rs.getString(i + 1));
        }
        try {
          records.add(objectMapper.writeValueAsString(rows));
          rows.clear();
        } catch (JsonProcessingException e) {
          e.printStackTrace();
        }
      }
      rs.close();
    } catch (SQLException ex) {
      LOGGER.error("Query Data Error - DFT Log");
      LOGGER.error("SQLException information");
      while (ex != null) {
        LOGGER.error("Error msg: " + ex.getMessage());
        ex = ex.getNextException();
      }
      throw new RuntimeException("Error");
    }
    return records;
  }

  @Override
  public int[] writeData(final WriteDataRequest request) {
    JsonNode recordJson;

    try (Connection conn = getConnection()) {
      // conn.setAutoCommit(true);
      Statement statement = conn.createStatement();

      String sql;

      for (String record : request.records()) {
        sql = "";
        try 
        {
          recordJson = objectMapper.readValue(record, JsonNode.class);
        } 
        catch (JsonProcessingException e) 
        {
          throw new IllegalArgumentException("Invalid record provided for Write operation. Record must be valid JSON", e);
        }
        List<String> keys = new ArrayList<>();
        Iterator<String> iterator = recordJson.fieldNames();
        iterator.forEachRemaining(e -> keys.add(e));

        if (WriteOperationType.INSERT.equals(request.operation()) || WriteOperationType.UPSERT.equals(request.operation())) 
        {
          if (WriteOperationType.UPSERT.equals(request.operation())) {
            sql = "REPLACE";
          } else {
            sql = "INSERT";
          }

          // sql += String.format(" INTO %s (%s) VALUES (", request.entityIdentifier(),
          // String.join(",", keys));
          sql += String.format(" INTO %s (%s) VALUES (", request.entityIdentifier(), String.join(",", keys));

          String value;

          for (int i = 0; i < keys.size(); i++) {
            value = StringEscapeUtils.escapeJava(getValueFromRecord(recordJson, keys.get(i)));
            if (i > 0) {
              sql += String.format(", \"%s\"", value);
            } else {
              sql += String.format("\"%s\"", value);
            }
          }
          sql += ")";
        } else if (WriteOperationType.UPDATE.equals(request.operation())) {
          if (Objects.requireNonNull(request.idFieldNames()).size() != 1) {
            throw new IllegalArgumentException("A single Id field is required for UPSERT operations in JDBC");
          }

          String recordIdKey = request.idFieldNames().get(0);
          String recordId = getValueFromRecord(recordJson, recordIdKey);
          String value;
          sql = String.format("UPDATE %s SET ", request.entityIdentifier());
          for (int i = 0; i < keys.size(); i++) {
            value = StringEscapeUtils.escapeJava(getValueFromRecord(recordJson, keys.get(i)));

            if (i > 0) {
              sql += String.format(", %s = \"%s\"", keys.get(i), value);
            } else {
              sql += String.format("%s = \"%s\"", keys.get(i), value);
            }
          }
          sql += String.format(" WHERE %s = %s", recordIdKey, recordId);
        }
        statement.addBatch(sql);
      }
      int[] records = statement.executeBatch();
      statement.close();
      conn.close();
      return records;
    } catch (SQLException ex) {
      LOGGER.error("SQLException information");
      while (ex != null) {
        LOGGER.error("Error msg: " + ex.getMessage());
        ex = ex.getNextException();
      }
      throw new RuntimeException("Error");
    }
  }

  private String getValueFromRecord(final JsonNode jsonRecord, final String key) {
    if (Objects.isNull(jsonRecord) || Objects.isNull(jsonRecord.get(key))) {
      throw new IllegalArgumentException(key + " key is missing from JSON record but is required");
    }
    if (StringUtils.isEmpty(jsonRecord.get(key).textValue())) {
      return null;
    }
    return jsonRecord.get(key).textValue();
  }
}
