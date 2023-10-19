package org.embulk.output.tsurugidb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.embulk.config.ConfigException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/JdbcSchema.java
public class TsurugiTableSchema {

    private List<TsurugiColumn> columns;

    @JsonCreator
    public TsurugiTableSchema(List<TsurugiColumn> columns) {
        this.columns = columns;
    }

    @JsonValue
    public List<TsurugiColumn> getColumns() {
        return columns;
    }

    public Optional<TsurugiColumn> findColumn(String name) {
        // because both upper case column and lower case column may exist, search twice
        for (TsurugiColumn column : columns) {
            if (column.getName().equals(name)) {
                return Optional.of(column);
            }
        }

        TsurugiColumn foundColumn = null;
        for (TsurugiColumn column : columns) {
            if (column.getName().equalsIgnoreCase(name)) {
                if (foundColumn != null) {
                    throw new ConfigException(String.format("Cannot specify column '%s' because both '%s' and '%s' exist.", name, foundColumn.getName(), column.getName()));
                }
                foundColumn = column;
            }
        }

        if (foundColumn != null) {
            return Optional.of(foundColumn);
        }
        return Optional.empty();
    }

    public int getCount() {
        return columns.size();
    }

    public TsurugiColumn getColumn(int i) {
        return columns.get(i);
    }

    public String getColumnName(int i) {
        return columns.get(i).getName();
    }

    public static TsurugiTableSchema filterSkipColumns(TsurugiTableSchema schema) {
        final ArrayList<TsurugiColumn> builder = new ArrayList<>();
        for (TsurugiColumn c : schema.getColumns()) {
            if (!c.isSkipColumn()) {
                builder.add(c);
            }
        }
        return new TsurugiTableSchema(Collections.unmodifiableList(builder));
    }
}
