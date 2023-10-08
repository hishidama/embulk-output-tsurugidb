package org.embulk.output.tsurugidb;

import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/TsurugiColumn.java
public class TsurugiColumn {

    private final int index;
    private final String name;
    private final String simpleTypeName;
    private final AtomType sqlType;
    private final int sizeTypeParameter;
    private final int scaleTypeParameter;
    private final int dataLength;
    private final Optional<String> declaredType;
    private final boolean isNotNull;
    private final boolean isUniqueKey;

    @JsonCreator
    public TsurugiColumn( //
            @JsonProperty("index") int index, //
            @JsonProperty("name") String name, //
            @JsonProperty("sqlType") AtomType sqlType, //
            @JsonProperty("simpleTypeName") String simpleTypeName, //
            @JsonProperty("sizeTypeParameter") int sizeTypeParameter, //
            @JsonProperty("scaleTypeParameter") int scaleTypeParameter, //
            @JsonProperty("dataLength") int dataLength, //
            @JsonProperty("declaredType") Optional<String> declaredType, //
            @JsonProperty("notNull") boolean isNotNull, //
            @JsonProperty("uniqueKey") boolean isUniqueKey) {
        this.index = index;
        this.name = name;
        this.simpleTypeName = simpleTypeName;
        this.sqlType = sqlType;
        this.sizeTypeParameter = sizeTypeParameter;
        this.scaleTypeParameter = scaleTypeParameter;
        this.dataLength = dataLength;
        this.declaredType = declaredType;
        this.isNotNull = isNotNull;
        this.isUniqueKey = isUniqueKey;
    }

    public static TsurugiColumn newGenericTypeColumn(int index, String name, AtomType sqlType, String simpleTypeName, int sizeTypeParameter, int scaleTypeParameter, int dataLength, boolean isNotNull,
            boolean isUniqueKey) {
        Objects.requireNonNull(name, "name==null");
        return new TsurugiColumn(index, name, sqlType, simpleTypeName, sizeTypeParameter, scaleTypeParameter, dataLength, Optional.<String>empty(), isNotNull, isUniqueKey);
    }

    public static TsurugiColumn newGenericTypeColumn(int index, String name, AtomType sqlType, String simpleTypeName, int sizeTypeParameter, int scaleTypeParameter, boolean isNotNull,
            boolean isUniqueKey) {
        Objects.requireNonNull(name, "name==null");
        return new TsurugiColumn(index, name, sqlType, simpleTypeName, sizeTypeParameter, scaleTypeParameter, sizeTypeParameter, Optional.<String>empty(), isNotNull, isUniqueKey);
    }

    public static TsurugiColumn newTypeDeclaredColumn(int index, String name, AtomType sqlType, String declaredType, boolean isNotNull, boolean isUniqueKey) {
        Objects.requireNonNull(name, "name==null");
        return new TsurugiColumn(index, name, sqlType, declaredType, 0, 0, 0, Optional.of(declaredType), isNotNull, isUniqueKey);
    }

    @JsonIgnore
    public static TsurugiColumn skipColumn() {
        return new TsurugiColumn(-1, null, AtomType.UNKNOWN, null, 0, 0, 0, Optional.<String>empty(), false, false);
    }

    @JsonIgnore
    public boolean isSkipColumn() {
        return name == null;
    }

    @JsonProperty("index")
    public int getIndex() {
        return index;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("sqlType")
    public AtomType getSqlType() {
        return sqlType;
    }

    @JsonProperty("simpleTypeName")
    public String getSimpleTypeName() {
        return simpleTypeName;
    }

    @JsonProperty("sizeTypeParameter")
    public int getSizeTypeParameter() {
        return sizeTypeParameter;
    }

    @JsonProperty("scaleTypeParameter")
    public int getScaleTypeParameter() {
        return scaleTypeParameter;
    }

    @JsonProperty("dataLength")
    public int getDataLength() {
        return dataLength;
    }

    @JsonProperty("declaredType")
    public Optional<String> getDeclaredType() {
        return declaredType;
    }

    @JsonProperty("notNull")
    public boolean isNotNull() {
        return isNotNull;
    }

    @JsonProperty("uniqueKey")
    public boolean isUniqueKey() {
        return isUniqueKey;
    }
}
