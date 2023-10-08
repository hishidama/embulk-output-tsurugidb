package org.embulk.output.tsurugidb.common;

import com.fasterxml.jackson.annotation.JsonProperty;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/TableIdentifier.java
public class TableIdentifier {

    private String database;
    private String schemaName;
    private String tableName;

    public TableIdentifier(String database, String schemaName, String tableName) {
        this.database = database;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public TableIdentifier() {
    }

    @JsonProperty
    public String getDatabase() {
        return database;
    }

    @JsonProperty
    public void setDatabase(String database) {
        this.database = database;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
