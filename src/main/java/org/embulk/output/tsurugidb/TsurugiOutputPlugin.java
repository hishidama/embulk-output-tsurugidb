package org.embulk.output.tsurugidb;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.output.tsurugidb.common.DbColumnOption;
import org.embulk.output.tsurugidb.common.MergeConfig;
import org.embulk.output.tsurugidb.common.PageReaderRecord;
import org.embulk.output.tsurugidb.common.TableIdentifier;
import org.embulk.output.tsurugidb.common.ToStringMap;
import org.embulk.output.tsurugidb.executor.TsurugiSqlExecutor;
import org.embulk.output.tsurugidb.insert.BatchInsert;
import org.embulk.output.tsurugidb.insert.InsertMethod;
import org.embulk.output.tsurugidb.insert.InsertMethodSub;
import org.embulk.output.tsurugidb.insert.TsurugiBatchInsertInsert;
import org.embulk.output.tsurugidb.insert.TsurugiBatchInsertInsertBatch;
import org.embulk.output.tsurugidb.insert.TsurugiBatchInsertInsertWait;
import org.embulk.output.tsurugidb.insert.TsurugiBatchInsertPut;
import org.embulk.output.tsurugidb.insert.TsurugiBatchInsertPutBatch;
import org.embulk.output.tsurugidb.insert.TsurugiBatchInsertPutWait;
import org.embulk.output.tsurugidb.setter.ColumnSetter;
import org.embulk.output.tsurugidb.setter.ColumnSetterFactory;
import org.embulk.output.tsurugidb.setter.ColumnSetterVisitor;
import org.embulk.output.tsurugidb.setter.Record;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.modules.ZoneIdModule;
import org.embulk.util.retryhelper.RetryExecutor;
import org.embulk.util.retryhelper.RetryGiveupException;
import org.embulk.util.retryhelper.Retryable;
import org.msgpack.core.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.exception.CcException;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/AbstractJdbcOutputPlugin.java
public class TsurugiOutputPlugin implements OutputPlugin {
    protected static final Logger logger = LoggerFactory.getLogger(TsurugiOutputPlugin.class);

    public static final String TYPE = "tsurugidb";

    protected static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().addModule(ZoneIdModule.withLegacyNames()).build();
    protected static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
    protected static final TaskMapper TASK_MAPPER = CONFIG_MAPPER_FACTORY.createTaskMapper();

    public interface PluginTask extends Task {
        @Config("endpoint")
        public String getEndpoint();

        @Config("user")
        @ConfigDefault("null")
        public Optional<String> getUser();

        @Config("password")
        @ConfigDefault("null")
        public Optional<String> getPassword();

        @Config("tx_type")
        @ConfigDefault("\"LTX\"") // OCC, LTX
        public String getTxType();

        @Config("tx_label")
        @ConfigDefault("\"embulk-output-tsurugidb\"")
        public String getTxLabel();

        @Config("tx_write_preserve")
        @ConfigDefault("[]")
        public List<String> getTxWritePreserve();

        @Config("tx_inclusive_read_area")
        @ConfigDefault("[]")
        public List<String> getTxInclusiveReadArea();

        @Config("tx_exclusive_read_area")
        @ConfigDefault("[]")
        public List<String> getTxExclusiveReadArea();

        @Config("tx_priority")
        @ConfigDefault("null")
        public Optional<String> getTxPriority();

        @Config("commit_type")
        @ConfigDefault("\"default\"")
        public TsurugiCommitType getCommitType();

        @Config("connect_timeout")
        @ConfigDefault("300")
        public int getConnectTimeout();

        @Config("begin_timeout")
        @ConfigDefault("300")
        public int getBeginTimeout();

        @Config("update_timeout")
        @ConfigDefault("300")
        public int getUpdateTimeout();

        @Config("insert_timeout")
        @ConfigDefault("300")
        public int getInsertTimeout();

        @Config("commit_timeout")
        @ConfigDefault("300")
        public int getCommitTimeout();

        @Config("socket_timeout")
        @ConfigDefault("1800")
        public int getSocketTimeout();

        @Config("options")
        @ConfigDefault("{}")
        public ToStringMap getOptions();

        @Config("table")
        public String getTable();

        @Config("mode")
        @ConfigDefault("\"insert_direct\"")
        public Mode getMode();

        @Config("method")
        @ConfigDefault("\"insert\"")
        public InsertMethod getInsertMethod();

        @Config("method_option")
        @ConfigDefault("null")
        public Optional<InsertMethodSub> getInsertMethodSub();

        @Config("batch_size")
        @ConfigDefault("16777216")
        // TODO set minimum number
        public int getBatchSize();

        @Config("merge_keys")
        @ConfigDefault("null")
        public Optional<List<String>> getMergeKeys();

        @Config("column_options")
        @ConfigDefault("{}")
        public Map<String, DbColumnOption> getColumnOptions();

        @Config("create_table_constraint")
        @ConfigDefault("null")
        public Optional<String> getCreateTableConstraint();

        @Config("create_table_option")
        @ConfigDefault("null")
        public Optional<String> getCreateTableOption();

        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        public ZoneId getDefaultTimeZone();

        @Config("retry_limit")
        @ConfigDefault("12")
        public int getRetryLimit();

        @Config("retry_wait")
        @ConfigDefault("1000")
        public int getRetryWait();

        @Config("max_retry_wait")
        @ConfigDefault("1800000") // 30 * 60 * 1000
        public int getMaxRetryWait();

        @Config("merge_rule")
        @ConfigDefault("null")
        public Optional<List<String>> getMergeRule();

        @Config("before_load")
        @ConfigDefault("null")
        public Optional<String> getBeforeLoad();

        @Config("after_load")
        @ConfigDefault("null")
        public Optional<String> getAfterLoad();

        public void setActualTable(TableIdentifier actualTable);

        public TableIdentifier getActualTable();

        public void setMergeKeys(Optional<List<String>> keys);

        public void setFeatures(Features features);

        public Features getFeatures();

        public Optional<TsurugiTableSchema> getNewTableSchema();

        public void setNewTableSchema(Optional<TsurugiTableSchema> schema);

        public TsurugiTableSchema getTargetTableSchema();

        public void setTargetTableSchema(TsurugiTableSchema schema);

        public Optional<List<TableIdentifier>> getIntermediateTables();

        public void setIntermediateTables(Optional<List<TableIdentifier>> names);
    }

    public static enum LengthSemantics {
        BYTES {
            @Override
            public int countLength(Charset charset, String s) {
                return charset.encode(s).remaining();
            }
        },
        CHARACTERS {
            @Override
            public int countLength(Charset charset, String s) {
                return s.length();
            }
        };

        public abstract int countLength(Charset charset, String s);
    }

    public static class Features {
        private int maxTableNameLength = 64;
        private LengthSemantics tableNameLengthSemantics = LengthSemantics.BYTES;
        private Set<Mode> supportedModes = Collections.unmodifiableSet(new HashSet<Mode>(Arrays.asList(Mode.values())));
        private boolean ignoreMergeKeys = false;

        public Features() {
        }

        @JsonProperty
        public int getMaxTableNameLength() {
            return maxTableNameLength;
        }

        @JsonProperty
        public Features setMaxTableNameLength(int bytes) {
            this.maxTableNameLength = bytes;
            return this;
        }

        public LengthSemantics getTableNameLengthSemantics() {
            return tableNameLengthSemantics;
        }

        @JsonProperty
        public Features setTableNameLengthSemantics(LengthSemantics tableNameLengthSemantics) {
            this.tableNameLengthSemantics = tableNameLengthSemantics;
            return this;
        }

        @JsonProperty
        public Set<Mode> getSupportedModes() {
            return supportedModes;
        }

        @JsonProperty
        public Features setSupportedModes(Set<Mode> modes) {
            this.supportedModes = modes;
            return this;
        }

        @JsonProperty
        public boolean getIgnoreMergeKeys() {
            return ignoreMergeKeys;
        }

        @JsonProperty
        public Features setIgnoreMergeKeys(boolean value) {
            this.ignoreMergeKeys = value;
            return this;
        }
    }

    protected Features getFeatures(PluginTask task) {
        // https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-postgresql/src/main/java/org/embulk/output/PostgreSQLOutputPlugin.java
        return new Features() //
                .setMaxTableNameLength(63) //
//              .setSupportedModes(Set.of(Mode.INSERT, Mode.INSERT_DIRECT, Mode.MERGE, Mode.MERGE_DIRECT, Mode.TRUNCATE_INSERT, Mode.REPLACE)) //
                .setSupportedModes(Set.of(Mode.INSERT_DIRECT)) //
                .setIgnoreMergeKeys(false); //
    }

    protected TsurugiOutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation) {
        // https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-postgresql/src/main/java/org/embulk/output/PostgreSQLOutputPlugin.java
        return new TsurugiOutputConnector(task);
    }

    protected BatchInsert newBatchInsert(PluginTask task, Optional<MergeConfig> mergeConfig) throws IOException, ServerException {
        // https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-postgresql/src/main/java/org/embulk/output/PostgreSQLOutputPlugin.java
        var connector = getConnector(task, true);
        var insertMethod = task.getInsertMethod();
        if (mergeConfig.isPresent()) {
            switch (insertMethod) {
            case INSERT:
                return new TsurugiBatchInsertInsert(connector, mergeConfig);
            case INSERT_WAIT:
                return new TsurugiBatchInsertInsertWait(connector, mergeConfig);
            case INSERT_BATCH:
                return new TsurugiBatchInsertInsertBatch(connector, mergeConfig);
            case PUT:
                return new TsurugiBatchInsertPut(connector, mergeConfig);
            case PUT_WAIT:
                return new TsurugiBatchInsertPutWait(connector, mergeConfig);
            case PUT_BATCH:
                return new TsurugiBatchInsertPutBatch(connector, mergeConfig);
            default:
                throw new AssertionError(insertMethod);
            }
        }
//      return new PostgreSQLCopyBatchInsert(getConnector(task, true));
        switch (insertMethod) {
        case INSERT:
            return new TsurugiBatchInsertInsert(connector, mergeConfig);
        case INSERT_WAIT:
            return new TsurugiBatchInsertInsertWait(connector, mergeConfig);
        case INSERT_BATCH:
            return new TsurugiBatchInsertInsertBatch(connector, mergeConfig);
        case PUT:
            return new TsurugiBatchInsertPut(connector, mergeConfig);
        case PUT_WAIT:
            return new TsurugiBatchInsertPutWait(connector, mergeConfig);
        case PUT_BATCH:
            return new TsurugiBatchInsertPutBatch(connector, mergeConfig);
        default:
            throw new AssertionError(insertMethod);
        }
    }

    protected TsurugiOutputConnection newConnection(PluginTask task, boolean retryableMetadataOperation, boolean autoCommit) throws ServerException {
        return TsurugiOutputConnection.newConnection(task, retryableMetadataOperation, autoCommit);
    }

    public enum Mode {
        INSERT, INSERT_DIRECT, MERGE, MERGE_DIRECT, TRUNCATE_INSERT, REPLACE;

        @JsonValue
        @Override
        public String toString() {
            return name().toLowerCase(Locale.ENGLISH);
        }

        @JsonCreator
        public static Mode fromString(String value) {
            switch (value) {
            case "insert":
                return INSERT;
            case "insert_direct":
                return INSERT_DIRECT;
            case "merge":
                return MERGE;
            case "merge_direct":
                return MERGE_DIRECT;
            case "truncate_insert":
                return TRUNCATE_INSERT;
            case "replace":
                return REPLACE;
            default:
                throw new ConfigException(String.format("Unknown mode '%s'. Supported modes are insert, insert_direct, merge, merge_direct, truncate_insert, replace", value));
            }
        }

        /**
         * @return True if this mode directly modifies the target table without creating intermediate tables.
         */
        public boolean isDirectModify() {
            return this == INSERT_DIRECT || this == MERGE_DIRECT;
        }

        /**
         * @return True if this mode merges records on unique keys
         */
        public boolean isMerge() {
            return this == MERGE || this == MERGE_DIRECT;
        }

        /**
         * @return True if this mode creates intermediate table for each tasks.
         */
        public boolean tempTablePerTask() {
            return this == INSERT || this == MERGE || this == TRUNCATE_INSERT /* this == REPLACE_VIEW */;
        }

        /**
         * @return True if this mode truncates the target table before committing intermediate tables
         */
        public boolean truncateBeforeCommit() {
            return this == TRUNCATE_INSERT;
        }

        /**
         * @return True if this mode uses MERGE statement to commit intermediate tables to the target table
         */
        public boolean commitByMerge() {
            return this == MERGE;
        }

        /**
         * @return True if this mode overwrites schema of the target tables
         */
        public boolean ignoreTargetTableSchema() {
            return this == REPLACE /* || this == REPLACE_VIEW */;
        }

        /**
         * @return True if this mode swaps the target tables with intermediate tables to commit
         */
        public boolean commitBySwapTable() {
            return this == REPLACE;
        }
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, Schema schema, int taskCount, Control control) {
        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

        Features features = getFeatures(task);
        task.setFeatures(features);

        if (!features.getSupportedModes().contains(task.getMode())) {
            throw new ConfigException(String.format("This output type doesn't support '%s'. Supported modes are: %s", task.getMode(), features.getSupportedModes()));
        }

        task = begin(task, schema, taskCount);
        control.run(task.toTaskSource());
        return commit(task, schema, taskCount);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, Schema schema, int taskCount, Control control) {
        PluginTask task = TASK_MAPPER.map(taskSource, PluginTask.class);

        if (!task.getMode().tempTablePerTask()) {
            throw new UnsupportedOperationException("inplace mode is not resumable. You need to delete partially-loaded records from the database and restart the entire transaction.");
        }

        task = begin(task, schema, taskCount);
        control.run(task.toTaskSource());
        return commit(task, schema, taskCount);
    }

    private PluginTask begin(final PluginTask task, final Schema schema, final int taskCount) {
        try {
            withRetry(task, new IdempotentSqlRunnable() { // no intermediate data if isDirectModify == true
                @Override
                public void run() throws ServerException {
                    TsurugiOutputConnection con = newConnection(task, true, false);
                    try {
                        doBegin(con, task, schema, taskCount);
                    } finally {
                        con.close();
                    }
                }
            });
        } catch (ServerException e) {
            throw new ServerRuntimeException(e);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        return task;
    }

    private ConfigDiff commit(final PluginTask task, Schema schema, final int taskCount) {
        if (!task.getMode().isDirectModify() || task.getAfterLoad().isPresent()) { // no intermediate data if isDirectModify == true
            try {
                withRetry(task, new IdempotentSqlRunnable() {
                    @Override
                    public void run() throws ServerException {
                        TsurugiOutputConnection con = newConnection(task, false, false);
                        try {
                            doCommit(con, task, taskCount);
                        } finally {
                            con.close();
                        }
                    }
                });
            } catch (ServerException e) {
                throw new ServerRuntimeException(e);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }
        return CONFIG_MAPPER_FACTORY.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource, Schema schema, int taskCount, List<TaskReport> successTaskReports) {
        PluginTask task = TASK_MAPPER.map(taskSource, PluginTask.class);

        if (!task.getMode().isDirectModify()) { // no intermediate data if isDirectModify == true
            try {
                withRetry(task, new IdempotentSqlRunnable() {
                    @Override
                    public void run() throws ServerException {
                        TsurugiOutputConnection con = newConnection(task, true, true);
                        try {
                            doCleanup(con, task, taskCount, successTaskReports);
                        } finally {
                            con.close();
                        }
                    }
                });
            } catch (ServerException e) {
                throw new ServerRuntimeException(e);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    protected void doBegin(TsurugiOutputConnection con, PluginTask task, final Schema schema, int taskCount) throws ServerException {
        if (schema.getColumnCount() == 0) {
            throw new ConfigException("No column.");
        }

        Mode mode = task.getMode();
        logger.info("Using {} mode", mode);

        if (mode.commitBySwapTable() && task.getBeforeLoad().isPresent()) {
            throw new ConfigException(String.format("%s mode does not support 'before_load' option.", mode));
        }

        String actualTable;
        if (con.tableExists(task.getTable())) {
            actualTable = task.getTable();
        } else {
            String upperTable = task.getTable().toUpperCase();
            String lowerTable = task.getTable().toLowerCase();
            if (con.tableExists(upperTable)) {
                if (con.tableExists(lowerTable)) {
                    throw new ConfigException(String.format("Cannot specify table '%s' because both '%s' and '%s' exist.", task.getTable(), upperTable, lowerTable));
                } else {
                    actualTable = upperTable;
                }
            } else {
                if (con.tableExists(lowerTable)) {
                    actualTable = lowerTable;
                } else {
                    actualTable = task.getTable();
                }
            }
        }
        task.setActualTable(new TableIdentifier(null, con.getSchemaName(), actualTable));

        Optional<TsurugiTableSchema> initialTargetTableSchema = mode.ignoreTargetTableSchema() ? Optional.<TsurugiTableSchema>empty()
                : newTsurugiTableSchemaFromTableIfExists(con, task.getActualTable());

        // TODO get CREATE TABLE statement from task if set
        TsurugiTableSchema newTableSchema = applyColumnOptionsToNewTableSchema(initialTargetTableSchema.orElseGet(new Supplier<TsurugiTableSchema>() {
            @Override
            public TsurugiTableSchema get() {
                return newTsurugiTableSchemaForNewTable(schema);
            }
        }), task.getColumnOptions());

        // create intermediate tables
        if (!mode.isDirectModify()) {
            // create the intermediate tables here
            task.setIntermediateTables(Optional.<List<TableIdentifier>>of(createIntermediateTables(con, task, taskCount, newTableSchema)));
        } else {
            // direct modify mode doesn't need intermediate tables.
            task.setIntermediateTables(Optional.<List<TableIdentifier>>empty());
            if (task.getBeforeLoad().isPresent()) {
                con.executeSql(task.getBeforeLoad().get());
            }
        }

        // build TsurugiTableSchema from a table
        TsurugiTableSchema targetTableSchema;
        if (initialTargetTableSchema.isPresent()) {
            targetTableSchema = initialTargetTableSchema.get();
            task.setNewTableSchema(Optional.<TsurugiTableSchema>empty());
        } else if (task.getIntermediateTables().isPresent() && !task.getIntermediateTables().get().isEmpty()) {
            TableIdentifier firstItermTable = task.getIntermediateTables().get().get(0);
            targetTableSchema = newTsurugiTableSchemaFromTableIfExists(con, firstItermTable).get();
            task.setNewTableSchema(Optional.of(newTableSchema));
        } else {
            // also create the target table if not exists
            // CREATE TABLE IF NOT EXISTS xyz
            con.createTableIfNotExists(task.getActualTable(), newTableSchema, task.getCreateTableConstraint(), task.getCreateTableOption());
            targetTableSchema = newTsurugiTableSchemaFromTableIfExists(con, task.getActualTable()).get();
            task.setNewTableSchema(Optional.<TsurugiTableSchema>empty());
        }
        task.setTargetTableSchema(matchSchemaByColumnNames(schema, targetTableSchema));

        // validate column_options
        newColumnSetters(newColumnSetterFactory(null, task.getDefaultTimeZone()), // TODO create a dummy BatchInsert
                task.getTargetTableSchema(), schema, task.getColumnOptions());

        // normalize merge_key parameter for merge modes
        if (mode.isMerge()) {
            Optional<List<String>> mergeKeys = task.getMergeKeys();
            if (task.getFeatures().getIgnoreMergeKeys()) {
                if (mergeKeys.isPresent()) {
                    throw new ConfigException("This output type does not accept 'merge_key' option.");
                }
                task.setMergeKeys(Optional.<List<String>>of(Collections.emptyList()));
            } else if (mergeKeys.isPresent()) {
                if (task.getMergeKeys().get().isEmpty()) {
                    throw new ConfigException("Empty 'merge_keys' option is invalid.");
                }
                for (String key : mergeKeys.get()) {
                    if (!targetTableSchema.findColumn(key).isPresent()) {
                        throw new ConfigException(String.format("Merge key '%s' does not exist in the target table.", key));
                    }
                }
            } else {
                final ArrayList<String> builder = new ArrayList<>();
                for (TsurugiColumn column : targetTableSchema.getColumns()) {
                    if (column.isUniqueKey()) {
                        builder.add(column.getName());
                    }
                }
                task.setMergeKeys(Optional.<List<String>>of(Collections.unmodifiableList(builder)));
                if (task.getMergeKeys().get().isEmpty()) {
                    throw new ConfigException("Merging mode is used but the target table does not have primary keys. Please set merge_keys option.");
                }
            }
            logger.info("Using merge keys: {}", task.getMergeKeys().get());
        } else {
            task.setMergeKeys(Optional.<List<String>>empty());
        }
    }

    protected ColumnSetterFactory newColumnSetterFactory(BatchInsert batch, ZoneId defaultTimeZone) {
        return new ColumnSetterFactory(batch, defaultTimeZone);
    }

    protected TableIdentifier buildIntermediateTableId(final TsurugiOutputConnection con, PluginTask task, String tableName) {
        return new TableIdentifier(null, con.getSchemaName(), tableName);
    }

    private List<TableIdentifier> createIntermediateTables(final TsurugiOutputConnection con, final PluginTask task, final int taskCount, final TsurugiTableSchema newTableSchema)
            throws ServerException {
        try {
            return buildRetryExecutor(task).run(new Retryable<List<TableIdentifier>>() {
                private TableIdentifier table;
                private ArrayList<TableIdentifier> intermTables;

                @Override
                public List<TableIdentifier> call() throws Exception {
                    intermTables = new ArrayList<>();
                    if (task.getMode().tempTablePerTask()) {
                        String tableNameFormat = generateIntermediateTableNameFormat(task.getActualTable().getTableName(), con, taskCount, task.getFeatures().getMaxTableNameLength(),
                                task.getFeatures().getTableNameLengthSemantics());
                        for (int taskIndex = 0; taskIndex < taskCount; taskIndex++) {
                            String tableName = String.format(tableNameFormat, taskIndex);
                            table = buildIntermediateTableId(con, task, tableName);
                            // if table already exists, ServerException will be thrown
                            con.createTable(table, newTableSchema, task.getCreateTableConstraint(), task.getCreateTableOption());
                            intermTables.add(table);
                        }
                    } else {
                        String tableName = generateIntermediateTableNamePrefix(task.getActualTable().getTableName(), con, 0, task.getFeatures().getMaxTableNameLength(),
                                task.getFeatures().getTableNameLengthSemantics());
                        table = buildIntermediateTableId(con, task, tableName);
                        con.createTable(table, newTableSchema, task.getCreateTableConstraint(), task.getCreateTableOption());
                        intermTables.add(table);
                    }
                    return Collections.unmodifiableList(intermTables);
                }

                @Override
                public boolean isRetryableException(Exception exception) {
                    for (Throwable t = exception; t != null; t = t.getCause()) {
                        if (t instanceof ServerException) {
                            try {
                                // true means that creating table failed because the table already exists.
                                return con.tableExists(table);
                            } catch (ServerException e) {
                            }
                        }
                    }
                    return false;
                }

                @Override
                public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait) throws RetryGiveupException {
                    logger.info("Try to create intermediate tables again because already exist");
                    try {
                        dropTables();
                    } catch (ServerException e) {
                        throw new RetryGiveupException(e);
                    }
                }

                @Override
                public void onGiveup(Exception firstException, Exception lastException) throws RetryGiveupException {
                    try {
                        dropTables();
                    } catch (ServerException e) {
                        logger.warn("Cannot delete intermediate table", e);
                    }
                }

                private void dropTables() throws ServerException {
                    for (TableIdentifier table : intermTables) {
                        con.dropTableIfExists(table);
                    }
                }
            });
        } catch (RetryGiveupException e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    static int calculateSuffixLength(int taskCount) {
        assert (taskCount >= 0);
        // NOTE: for backward compatibility
        // See. https://github.com/embulk/embulk-output-jdbc/pull/301
        int minimumLength = 3;
        return Math.max(minimumLength, String.valueOf(taskCount - 1).length());
    }

    protected String generateIntermediateTableNameFormat(String baseTableName, TsurugiOutputConnection con, int taskCount, int maxLength, LengthSemantics lengthSemantics) throws ServerException {
        int suffixLength = calculateSuffixLength(taskCount);
        String prefix = generateIntermediateTableNamePrefix(baseTableName, con, suffixLength, maxLength, lengthSemantics);
        String suffixFormat = "%0" + suffixLength + "d";
        return prefix + suffixFormat;
    }

    protected String generateIntermediateTableNamePrefix(String baseTableName, TsurugiOutputConnection con, int suffixLength, int maxLength, LengthSemantics lengthSemantics) throws ServerException {
        Charset tableNameCharset = con.getTableNameCharset();
        String tableName = baseTableName;
        String suffix = "_embulk";
        String uniqueSuffix = String.format("%016x", System.currentTimeMillis()) + suffix;

        // way to count length of table name varies by DBMSs (bytes or characters),
        // so truncate swap table name by one character.
        while (!checkTableNameLength(tableName + "_" + uniqueSuffix, tableNameCharset, suffixLength, maxLength, lengthSemantics)) {
            if (uniqueSuffix.length() > 8 + suffix.length()) {
                // truncate transaction unique name
                // (include 8 characters of the transaction name at least)
                uniqueSuffix = uniqueSuffix.substring(1);
            } else {
                if (tableName.isEmpty()) {
                    throw new ConfigException("Table name is too long to generate temporary table name");
                }
                // truncate table name
                tableName = tableName.substring(0, tableName.length() - 1);
                // if (!connection.tableExists(tableName)) {
                // TODO this doesn't help. Rather than truncating more characters,
                // here needs to replace characters with random characters. But
                // to make the result deterministic. So, an idea is replacing
                // the last character to the first (second, third, ... for each loop)
                // of md5(original table name).
                // }
            }

        }
        return tableName + "_" + uniqueSuffix;
    }

    private static TsurugiTableSchema applyColumnOptionsToNewTableSchema(TsurugiTableSchema schema, Map<String, DbColumnOption> columnOptions) {
        return new TsurugiTableSchema(schema.getColumns().stream().map(c -> {
            DbColumnOption option = columnOptionOf(columnOptions, c.getName());
            if (option.getType().isPresent()) {
                return TsurugiColumn.newTypeDeclaredColumn(c.getIndex(), c.getName(), AtomType.UNKNOWN, // sqlType, isNotNull, and isUniqueKey are ignored
                        option.getType().get(), false, false);
            }
            return c;
        }).collect(Collectors.toList()));
    }

    protected static List<ColumnSetter> newColumnSetters(ColumnSetterFactory factory, TsurugiTableSchema targetTableSchema, Schema inputValueSchema, Map<String, DbColumnOption> columnOptions) {
        final ArrayList<ColumnSetter> builder = new ArrayList<>();
        for (int schemaColumnIndex = 0; schemaColumnIndex < targetTableSchema.getCount(); schemaColumnIndex++) {
            TsurugiColumn targetColumn = targetTableSchema.getColumn(schemaColumnIndex);
            Column inputColumn = inputValueSchema.getColumn(schemaColumnIndex);

            String bindName = TsurugiSqlExecutor.getBindName(targetColumn);
            if (targetColumn.isSkipColumn()) {
                builder.add(factory.newSkipColumnSetter(bindName));
            } else {
                DbColumnOption option = columnOptionOf(columnOptions, inputColumn.getName());
                builder.add(factory.newColumnSetter(bindName, targetColumn, option));
            }
        }
        return Collections.unmodifiableList(builder);
    }

    private static DbColumnOption columnOptionOf(Map<String, DbColumnOption> columnOptions, String columnName) {
        return Optional.ofNullable(columnOptions.get(columnName)).orElseGet(
                // default column option
                new Supplier<DbColumnOption>() {
                    @Override
                    public DbColumnOption get() {
                        return CONFIG_MAPPER.map(CONFIG_MAPPER_FACTORY.newConfigSource(), DbColumnOption.class);
                    }
                });
    }

    private boolean checkTableNameLength(String tableName, Charset tableNameCharset, int suffixLength, int maxLength, LengthSemantics lengthSemantics) {
        return lengthSemantics.countLength(tableNameCharset, tableName) + suffixLength <= maxLength;
    }

    protected void doCommit(TsurugiOutputConnection con, PluginTask task, int taskCount) throws ServerException {
        TsurugiTableSchema schema = TsurugiTableSchema.filterSkipColumns(task.getTargetTableSchema());

        switch (task.getMode()) {
        case INSERT_DIRECT:
        case MERGE_DIRECT:
            // already loaded
            if (task.getAfterLoad().isPresent()) {
                con.executeSql(task.getAfterLoad().get());
            }
            break;

        case INSERT:
            // aggregate insert into target
            if (task.getNewTableSchema().isPresent()) {
                con.createTableIfNotExists(task.getActualTable(), task.getNewTableSchema().get(), task.getCreateTableConstraint(), task.getCreateTableOption());
            }
            con.collectInsert(task.getIntermediateTables().get(), schema, task.getActualTable(), false, task.getBeforeLoad(), task.getAfterLoad());
            break;

        case TRUNCATE_INSERT:
            // truncate & aggregate insert into target
            if (task.getNewTableSchema().isPresent()) {
                con.createTableIfNotExists(task.getActualTable(), task.getNewTableSchema().get(), task.getCreateTableConstraint(), task.getCreateTableOption());
            }
            con.collectInsert(task.getIntermediateTables().get(), schema, task.getActualTable(), true, task.getBeforeLoad(), task.getAfterLoad());
            break;

        case MERGE:
            // aggregate merge into target
            if (task.getNewTableSchema().isPresent()) {
                con.createTableIfNotExists(task.getActualTable(), task.getNewTableSchema().get(), task.getCreateTableConstraint(), task.getCreateTableOption());
            }
            con.collectMerge(task.getIntermediateTables().get(), schema, task.getActualTable(), new MergeConfig(task.getMergeKeys().get(), task.getMergeRule()), task.getBeforeLoad(),
                    task.getAfterLoad());
            break;

        case REPLACE:
            // swap table
            con.replaceTable(task.getIntermediateTables().get().get(0), schema, task.getActualTable(), task.getAfterLoad());
            break;
        }
    }

    protected void doCleanup(TsurugiOutputConnection con, PluginTask task, int taskCount, List<TaskReport> successTaskReports) throws ServerException {
        if (task.getIntermediateTables().isPresent()) {
            for (TableIdentifier intermTable : task.getIntermediateTables().get()) {
                con.dropTableIfExists(intermTable);
            }
        }
    }

    protected TsurugiTableSchema newTsurugiTableSchemaForNewTable(Schema schema) {
        final ArrayList<TsurugiColumn> columns = new ArrayList<>();
        int i = 0;
        for (Column c : schema.getColumns()) {
            final int index = i++;
            final String columnName = c.getName();
            c.visit(new ColumnVisitor() {
                @Override
                public void booleanColumn(Column column) {
                    columns.add(TsurugiColumn.newGenericTypeColumn(index, columnName, AtomType.BOOLEAN, "BOOLEAN", 1, 0, false, false));
                }

                @Override
                public void longColumn(Column column) {
                    columns.add(TsurugiColumn.newGenericTypeColumn(index, columnName, AtomType.INT8, "BIGINT", 22, 0, false, false));
                }

                @Override
                public void doubleColumn(Column column) {
                    columns.add(TsurugiColumn.newGenericTypeColumn(index, columnName, AtomType.FLOAT8, "DOUBLE PRECISION", 24, 0, false, false));
                }

                @Override
                public void stringColumn(Column column) {
                    columns.add(TsurugiColumn.newGenericTypeColumn(index, columnName, AtomType.CLOB, "CLOB", 4000, 0, false, false)); // TODO size type param
                }

                @Override
                public void jsonColumn(Column column) {
                    columns.add(TsurugiColumn.newGenericTypeColumn(index, columnName, AtomType.CLOB, "CLOB", 4000, 0, false, false)); // TODO size type param
                }

                @Override
                public void timestampColumn(Column column) {
                    columns.add(TsurugiColumn.newGenericTypeColumn(index, columnName, AtomType.TIME_POINT_WITH_TIME_ZONE, "TIMESTAMP", 26, 0, false, false)); // size type param is from postgresql
                }
            });
        }
        return new TsurugiTableSchema(Collections.unmodifiableList(columns));
    }

    public Optional<TsurugiTableSchema> newTsurugiTableSchemaFromTableIfExists(TsurugiOutputConnection connection, TableIdentifier table) throws ServerException {
        var metadataOpt = connection.findTableMetadata(table.getTableName());
        if (metadataOpt.isEmpty()) {
            // DatabaseMetaData.getPrimaryKeys fails if table does not exist
            return Optional.empty();
        }
        var metadata = metadataOpt.get();

        var columns = new ArrayList<TsurugiColumn>();
        int i = 0;
        for (var column : metadata.getColumns()) {
            String columnName = column.getName();
            var sqlType = column.getAtomType();
            // TODO Tsurugi column metadata
            String simpleTypeName = sqlType.toString();
//          boolean isUniqueKey = primaryKeys.contains(columnName);
            boolean isUniqueKey = false;
//          int colSize = rs.getInt("COLUMN_SIZE");
            int colSize = 0;
//          int decDigit = rs.getInt("DECIMAL_DIGITS");
//          if (rs.wasNull()) {
//              decDigit = -1;
//          }
            int decDigit = 0;
//          int charOctetLength = rs.getInt("CHAR_OCTET_LENGTH");
            int charOctetLength = 0;
//          boolean isNotNull = "NO".equals(rs.getString("IS_NULLABLE"));
            boolean isNotNull = false;

            columns.add(TsurugiColumn.newGenericTypeColumn(i, columnName, sqlType, simpleTypeName, colSize, decDigit, charOctetLength, isNotNull, isUniqueKey));
            i++;
        }

        if (columns.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(new TsurugiTableSchema(Collections.unmodifiableList(columns)));
        }
    }

    private TsurugiTableSchema matchSchemaByColumnNames(Schema inputSchema, TsurugiTableSchema targetTableSchema) {
        final ArrayList<TsurugiColumn> tsurugiColumns = new ArrayList<>();

        for (Column column : inputSchema.getColumns()) {
            Optional<TsurugiColumn> c = targetTableSchema.findColumn(column.getName());
            tsurugiColumns.add(c.orElse(TsurugiColumn.skipColumn()));
        }

        return new TsurugiTableSchema(Collections.unmodifiableList(tsurugiColumns));
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex) {
        PluginTask task = TASK_MAPPER.map(taskSource, PluginTask.class);
        Mode mode = task.getMode();

        // instantiate BatchInsert without table name
        BatchInsert batch = null;
        try {
            Optional<MergeConfig> config = Optional.empty();
            if (task.getMode() == Mode.MERGE_DIRECT) {
                config = Optional.of(new MergeConfig(task.getMergeKeys().get(), task.getMergeRule()));
            }
            batch = newBatchInsert(task, config);
        } catch (ServerException e) {
            throw new ServerRuntimeException(e);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        try {
            // configure PageReader -> BatchInsert
            PageReader reader = Exec.getPageReader(schema);

            List<ColumnSetter> columnSetters = newColumnSetters(newColumnSetterFactory(batch, task.getDefaultTimeZone()), task.getTargetTableSchema(), schema, task.getColumnOptions());
            TsurugiTableSchema insertIntoSchema = TsurugiTableSchema.filterSkipColumns(task.getTargetTableSchema());
            if (insertIntoSchema.getCount() == 0) {
                throw new RuntimeException("No column to insert.");
            }

            // configure BatchInsert -> an intermediate table (!isDirectModify) or the target table (isDirectModify)
            TableIdentifier destTable;
            if (mode.tempTablePerTask()) {
                destTable = task.getIntermediateTables().get().get(taskIndex);
            } else if (mode.isDirectModify()) {
                destTable = task.getActualTable();
            } else {
                destTable = task.getIntermediateTables().get().get(0);
            }
            batch.prepare(destTable, insertIntoSchema);

            PluginPageOutput output = new PluginPageOutput(reader, batch, columnSetters, task.getBatchSize(), task);
            batch = null;
            return output;

        } catch (ServerException e) {
            throw new ServerRuntimeException(e);

        } finally {
            if (batch != null) {
                try {
                    batch.close();
                } catch (ServerException e) {
                    throw new ServerRuntimeException(e);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

//    public static File findPluginRoot(Class<?> cls) {
//        try {
//            URL url = cls.getResource("/" + cls.getName().replace('.', '/') + ".class");
//            if (url.toString().startsWith("jar:")) {
//                url = new URL(url.toString().replaceAll("^jar:", "").replaceAll("![^!]*$", ""));
//            }
//
//            File folder = new File(url.toURI()).getParentFile();
//            for (;; folder = folder.getParentFile()) {
//                if (folder == null) {
//                    throw new RuntimeException("Cannot find 'embulk-output-xxx' folder.");
//                }
//
//                if (folder.getName().startsWith("embulk-output-")) {
//                    return folder;
//                }
//            }
//        } catch (MalformedURLException | URISyntaxException e) {
//            throw new RuntimeException(e);
//        }
//    }

    public class PluginPageOutput implements TransactionalPageOutput {
        protected final List<Column> columns;
        protected final List<ColumnSetter> columnSetters;
        protected final List<ColumnSetterVisitor> columnVisitors;
        private final PageReaderRecord pageReader;
        private final BatchInsert batch;
        private final int batchSize;
        private final int forceBatchFlushSize;
        private final PluginTask task;

        public PluginPageOutput(PageReader pageReader, BatchInsert batch, List<ColumnSetter> columnSetters, int batchSize, PluginTask task) {
            this.pageReader = new PageReaderRecord(pageReader);
            this.batch = batch;
            this.columns = pageReader.getSchema().getColumns();
            this.columnSetters = columnSetters;

            this.columnVisitors = Collections.unmodifiableList(columnSetters.stream().map(setter -> {
                return new ColumnSetterVisitor(PluginPageOutput.this.pageReader, setter);
            }).collect(Collectors.toList()));
            this.batchSize = batchSize;
            this.task = task;
            this.forceBatchFlushSize = batchSize * 2;
        }

        @Override
        public void add(Page page) {
            try {
                pageReader.setPage(page);
                while (pageReader.nextRecord()) {
                    if (batch.getBatchWeight() > forceBatchFlushSize) {
                        flush();
                    }
                    batch.beforeAdd();
                    handleColumnsSetters();
                    batch.add();
                }
                if (batch.getBatchWeight() > batchSize) {
                    flush();
                }
            } catch (ServerException e) {
                throw new ServerRuntimeException(e);
            } catch (IOException | InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }

        private void flush() throws ServerException, InterruptedException {
            withRetry(task, new IdempotentSqlRunnable() {
                private boolean first = true;

                @Override
                public void run() throws IOException, ServerException {
                    try {
                        if (!first) {
                            retryColumnsSetters();
                        }

                        batch.flush();

                    } catch (IOException | ServerException ex) {
                        if (!first && !isRetryableException(ex)) {
                            logger.error("Retry failed : ", ex);
                        }
                        throw ex;
                    } finally {
                        first = false;
                    }
                }
            });

            pageReader.clearReadRecords();
        }

        @Override
        public void finish() {
            try {
                flush();

                withRetry(task, new IdempotentSqlRunnable() {
                    @Override
                    public void run() throws IOException, ServerException {
                        batch.finish();
                    }
                });
            } catch (ServerException e) {
                throw new ServerRuntimeException(e);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void close() {
            try {
                batch.close();
            } catch (ServerException e) {
                throw new ServerRuntimeException(e);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void abort() {
        }

        @Override
        public TaskReport commit() {
            return CONFIG_MAPPER_FACTORY.newTaskReport();
        }

        protected void handleColumnsSetters() {
            int size = columnVisitors.size();
            for (int i = 0; i < size; i++) {
                columns.get(i).visit(columnVisitors.get(i));
            }
        }

        protected void retryColumnsSetters() throws IOException, ServerException {
            int size = columnVisitors.size();
            int[] updateCounts = batch.getLastUpdateCounts();
            int index = 0;
            for (Iterator<? extends Record> it = pageReader.getReadRecords().iterator(); it.hasNext();) {
                Record record = it.next();
                // retry failed records
                if (index >= updateCounts.length /* || updateCounts[index] == Statement.EXECUTE_FAILED */) {
                    batch.beforeAdd();
                    for (int i = 0; i < size; i++) {
                        ColumnSetterVisitor columnVisitor = new ColumnSetterVisitor(record, columnSetters.get(i));
                        columns.get(i).visit(columnVisitor);
                    }
                    batch.add();
                } else {
                    // remove for re-retry
                    it.remove();
                }
                index++;
            }
        }
    }

    protected boolean isRetryableException(Exception exception) {
        var ex = findServerException(exception);
        if (ex != null) {
            return isRetryableException(ex);
        } else {
            return false;
        }
    }

    protected boolean isRetryableException(ServerException ex) {
        return ex instanceof CcException;
    }

    public static interface IdempotentSqlRunnable {
        public void run() throws IOException, ServerException;
    }

    protected void withRetry(PluginTask task, IdempotentSqlRunnable op) throws ServerException, InterruptedException {
        withRetry(task, op, "Operation failed");
    }

    protected void withRetry(PluginTask task, final IdempotentSqlRunnable op, final String errorMessage) throws ServerException, InterruptedException {
        try {
            buildRetryExecutor(task).runInterruptible(new RetryableSQLExecution(op, errorMessage));
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof ServerException) {
                throw (ServerException) cause;
            } else if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else if (cause instanceof Error) {
                throw (Error) cause;
            } else {
                throw new RuntimeException(cause);
            }
        }
    }

    private static RetryExecutor buildRetryExecutor(PluginTask task) {
        return RetryExecutor.builder().withRetryLimit(0).withRetryLimit(task.getRetryLimit()).withInitialRetryWaitMillis(task.getRetryWait()).withMaxRetryWaitMillis(task.getMaxRetryWait()).build();
    }

    class RetryableSQLExecution implements Retryable<Void> {
        private final String errorMessage;
        private final IdempotentSqlRunnable op;

        private final Logger logger = LoggerFactory.getLogger(RetryableSQLExecution.class);

        public RetryableSQLExecution(IdempotentSqlRunnable op, String errorMessage) {
            this.errorMessage = errorMessage;
            this.op = op;
        }

        @Override
        public Void call() throws Exception {
            op.run();
            return null;
        }

        @Override
        public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait) {
            var ex = findServerException(exception);
            if (ex != null) {
                var errorCode = ex.getDiagnosticCode();
                logger.warn("{} ({}:{}), retrying {}/{} after {} seconds. Message: {}", errorMessage, errorCode.name(), errorCode, retryCount, retryLimit, retryWait / 1000,
                        buildExceptionMessage(exception));
            } else {
                logger.warn("{}, retrying {}/{} after {} seconds. Message: {}", errorMessage, retryCount, retryLimit, retryWait / 1000, buildExceptionMessage(exception));
            }
            if (retryCount % 3 == 0) {
                logger.info("Error details:", exception);
            }
        }

        @Override
        public void onGiveup(Exception firstException, Exception lastException) {
            var ex = findServerException(firstException);
            if (ex != null) {
                var errorCode = ex.getDiagnosticCode();
                logger.error("{} ({}:{})", errorMessage, errorCode.name(), errorCode);
            }
        }

        @Override
        public boolean isRetryableException(Exception exception) {
            return TsurugiOutputPlugin.this.isRetryableException(exception);
        }

        private String buildExceptionMessage(Throwable ex) {
            StringBuilder sb = new StringBuilder();
            sb.append(ex.getMessage());
            if (ex.getCause() != null) {
                buildExceptionMessageCont(sb, ex.getCause(), ex.getMessage());
            }
            return sb.toString();
        }

        private void buildExceptionMessageCont(StringBuilder sb, Throwable ex, String lastMessage) {
            if (!lastMessage.equals(ex.getMessage())) {
                // suppress same messages
                sb.append(" < ");
                sb.append(ex.getMessage());
            }
            if (ex.getCause() == null) {
                return;
            }
            buildExceptionMessageCont(sb, ex.getCause(), ex.getMessage());
        }
    }

    static ServerException findServerException(Exception exception) {
        for (Throwable t = exception; t != null; t = t.getCause()) {
            if (t instanceof ServerException) {
                return (ServerException) t;
            }
        }
        return null;
    }
}
