package org.embulk.output.tsurugidb.executor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.embulk.config.ConfigException;
import org.embulk.output.tsurugidb.TsurugiColumn;
import org.embulk.output.tsurugidb.TsurugiOutputPlugin.PluginTask;
import org.embulk.output.tsurugidb.TsurugiTableSchema;
import org.embulk.output.tsurugidb.common.MergeConfig;
import org.embulk.output.tsurugidb.common.TableIdentifier;
import org.embulk.output.tsurugidb.insert.InsertMethodSub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.sql.proto.SqlRequest.CommitStatus;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.sql.proto.SqlRequest.Placeholder;
import com.tsurugidb.sql.proto.SqlRequest.ReadArea;
import com.tsurugidb.sql.proto.SqlRequest.TransactionOption;
import com.tsurugidb.sql.proto.SqlRequest.TransactionPriority;
import com.tsurugidb.sql.proto.SqlRequest.TransactionType;
import com.tsurugidb.sql.proto.SqlRequest.WritePreserve;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.ExecuteResult;
import com.tsurugidb.tsubakuro.sql.Placeholders;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.TableMetadata;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.sql.exception.TargetNotFoundException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

//https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/JdbcOutputConnection.java
public class TsurugiSqlExecutor implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(TsurugiSqlExecutor.class);

    public static TsurugiSqlExecutor create(PluginTask task, Session session, boolean autoCommit) {
        var txOption = getTxOption(task);
        logger.debug("txOption={}", txOption);
        var commitStatus = task.getCommitType().toCommitStatus();
        logger.debug("commitStatus={}", commitStatus);

        return new TsurugiSqlExecutor(task, session, txOption, commitStatus, autoCommit);
    }

    private static TransactionOption getTxOption(PluginTask task) {
        var builder = TransactionOption.newBuilder();

        String txType = task.getTxType();
        switch (txType.toUpperCase()) {
        case "OCC":
            builder.setType(TransactionType.SHORT);
            break;
        case "LTX":
            builder.setType(TransactionType.LONG);
            String table = task.getTable();
            builder.addWritePreserves(WritePreserve.newBuilder().setTableName(table));
            builder.addInclusiveReadAreas(ReadArea.newBuilder().setTableName(table));
            task.getTxWritePreserve().forEach(tableName -> {
                builder.addWritePreserves(WritePreserve.newBuilder().setTableName(tableName));
            });
            task.getTxInclusiveReadArea().forEach(tableName -> {
                builder.addInclusiveReadAreas(ReadArea.newBuilder().setTableName(tableName));
            });
            task.getTxExclusiveReadArea().forEach(tableName -> {
                builder.addExclusiveReadAreas(ReadArea.newBuilder().setTableName(tableName));
            });
            fillPriority(builder, task);
            break;
        case "RTX":
            builder.setType(TransactionType.READ_ONLY);
            fillPriority(builder, task);
            break;
        default:
            throw new ConfigException("unsupported tx_type(" + txType + "). choose from OCC,LTX,RTX");
        }

        builder.setLabel(task.getTxLabel());
        return builder.build();
    }

    private static void fillPriority(TransactionOption.Builder builder, PluginTask task) {
        task.getTxPriority().ifPresent(s -> {
            TransactionPriority priority;
            try {
                priority = TransactionPriority.valueOf(s.toUpperCase());
            } catch (Exception e) {
                var ce = new ConfigException(MessageFormat.format("Unknown tx_priority ''{0}''. Supported tx_priority are {1}", //
                        s, Arrays.stream(TransactionPriority.values()).map(TransactionPriority::toString).map(String::toLowerCase).collect(Collectors.joining(", "))));
                ce.addSuppressed(e);
                throw ce;
            }
            builder.setPriority(priority);
        });
    }

    private final PluginTask task;
    private final SqlClient sqlClient;
    private final TransactionOption txOption;
    private final CommitStatus commitStatus;
    private final boolean autoCommit;
    private Transaction transaction;
    protected String identifierQuoteString = "\"";

    public TsurugiSqlExecutor(PluginTask task, Session session, TransactionOption txOption, CommitStatus commitStatus, boolean autoCommit) {
        this.task = task;
        this.sqlClient = SqlClient.attach(session);
        this.txOption = txOption;
        this.commitStatus = commitStatus;
        this.autoCommit = autoCommit;
    }

    protected synchronized Transaction getTransaction() throws ServerException {
        if (transaction == null) {
            int timeout = task.getBeginTimeout();
            try {
                transaction = sqlClient.createTransaction(txOption).await(timeout, TimeUnit.SECONDS);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            } catch (InterruptedException | TimeoutException e) {
                throw new RuntimeException(e);
            }
            transaction.setCloseTimeout(timeout, TimeUnit.SECONDS);
        }
        return transaction;
    }

    public Optional<TableMetadata> findTableMetadata(String tableName) throws ServerException {
        try {
            int timeout = task.getConnectTimeout();
            var metadata = sqlClient.getTableMetadata(tableName).await(timeout, TimeUnit.SECONDS);
            return Optional.of(metadata);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (TargetNotFoundException e) {
            return Optional.empty();
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public int executeUpdate(String sql) throws ServerException {
        var tx = getTransaction();
        logger.info("SQL: " + sql);
        long startTime = System.currentTimeMillis();
        int count = 0;
        try {
            int timeout = task.getUpdateTimeout();
            tx.executeStatement(sql).await(timeout, TimeUnit.SECONDS);
            // TODO Tsurugi get count
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }

        if (autoCommit) {
            try {
                commit();
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }

        double seconds = (System.currentTimeMillis() - startTime) / 1000.0;
        if (count == 0) {
            logger.info(String.format("> %.2f seconds", seconds));
        } else {
            logger.info(String.format("> %.2f seconds (%,d rows)", seconds, count));
        }
        return count;
    }

    public PreparedStatement prepareBatchInsertStatement(TableIdentifier toTable, TsurugiTableSchema toTableSchema, Optional<MergeConfig> mergeConfig) throws ServerException {
        String sql;
        if (mergeConfig.isPresent()) {
            sql = buildPreparedMergeSql(toTable, toTableSchema, mergeConfig.get());
        } else {
            sql = buildPreparedInsertSql(toTable, toTableSchema);
        }
        logger.info("Prepared SQL: {}", sql);

        var placeholders = new ArrayList<Placeholder>();
        for (int i = 0; i < toTableSchema.getCount(); i++) {
            TsurugiColumn column = toTableSchema.getColumn(i);
            String bindName = getBindName(column);
            placeholders.add(Placeholders.of(bindName, column.getSqlType()));
        }

        return prepare(sql, placeholders);
    }

    protected String buildPreparedInsertSql(TableIdentifier toTable, TsurugiTableSchema toTableSchema) {
        StringBuilder sb = new StringBuilder();

        sb.append(getInsertInstruction());
        sb.append(" INTO ");
        quoteTableIdentifier(sb, toTable);

        sb.append(" (");
        for (int i = 0; i < toTableSchema.getCount(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            quoteIdentifierString(sb, toTableSchema.getColumnName(i));
        }
        sb.append(") VALUES (");
        for (int i = 0; i < toTableSchema.getCount(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(":");
            sb.append(getBindName(toTableSchema.getColumn(i)));
        }
        sb.append(")");

        return sb.toString();
    }

    private static final Map<InsertMethodSub, String> INSERT_INSTRUCTION = Map.of( //
            InsertMethodSub.INSERT, "INSERT", //
            InsertMethodSub.INSERT_OR_REPLACE, "INSERT OR REPLACE", //
            InsertMethodSub.INSERT_IF_NOT_EXISTS, "INSERT IF NOT EXISTS");

    protected String getInsertInstruction() {
        var option = task.getInsertMethodSub().orElse(InsertMethodSub.INSERT);
        var instruction = INSERT_INSTRUCTION.get(option);
        if (instruction != null) {
            return instruction;
        }
        var message = InsertMethodSub.getUnsupportedMessage("insert", option, INSERT_INSTRUCTION.keySet());
        throw new ConfigException(message);
    }

    public String quoteTableIdentifier(TableIdentifier table) {
        StringBuilder sb = new StringBuilder();
        if (table.getDatabase() != null) {
            sb.append(quoteIdentifierString(table.getDatabase(), identifierQuoteString));
            sb.append(".");
        }
        if (table.getSchemaName() != null) {
            sb.append(quoteIdentifierString(table.getSchemaName(), identifierQuoteString));
            sb.append(".");
        }
        sb.append(quoteIdentifierString(table.getTableName(), identifierQuoteString));
        return sb.toString();
    }

    public void quoteTableIdentifier(StringBuilder sb, TableIdentifier table) {
        sb.append(quoteTableIdentifier(table));
    }

    public void quoteIdentifierString(StringBuilder sb, String str) {
        sb.append(quoteIdentifierString(str));
    }

    protected String quoteIdentifierString(String str) {
        return quoteIdentifierString(str, identifierQuoteString);
    }

    protected String quoteIdentifierString(String str, String quoteString) {
        // TODO if identifierQuoteString.equals(" ") && str.contains([^a-zA-Z0-9_connection.getMetaData().getExtraNameCharacters()])
        // TODO if str.contains(identifierQuoteString);
        return quoteString + str + quoteString;
    }

    public static String getBindName(TsurugiColumn column) {
        String name = column.getName();
        if (name == null) {
            throw new AssertionError("name==null");
        }
        if (isValidBindName(name)) {
            return name;
        }

        int index = column.getIndex();
        if (index < 0) {
            throw new AssertionError("index < 0");
        }
        return "_c" + index;
    }

    private static boolean isValidBindName(String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (Character.isAlphabetic(c) || c == '_') {
                continue;
            }
            if (Character.isDigit(c) && i > 0) {
                continue;
            }
            return false;
        }
        return true;
    }

    protected String buildPreparedMergeSql(TableIdentifier toTable, TsurugiTableSchema toTableSchema, MergeConfig mergeConfig) {
        throw new UnsupportedOperationException("not implemented");
    }

    public PreparedStatement prepare(String sql, ArrayList<Placeholder> placeholders) throws ServerException {
        try {
            int timeout = task.getConnectTimeout();
            return sqlClient.prepare(sql, placeholders).await(timeout, TimeUnit.SECONDS);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public int[] executeInsertWait(PreparedStatement ps, List<List<Parameter>> list) throws IOException, ServerException {
        var result = new int[list.size()];

        var tx = getTransaction();
        int i = 0;
        for (var parameter : list) {
            try {
                int timeout = task.getInsertTimeout();
                var executeResult = tx.executeStatement(ps, parameter).await(timeout, TimeUnit.SECONDS);
                result[i++] = toCount(executeResult);
            } catch (InterruptedException | TimeoutException e) {
                logExceptionRecord(e, parameter);
                throw new RuntimeException(e);
            } catch (Exception e) {
                logExceptionRecord(e, parameter);
                throw e;
            }
        }

        if (autoCommit) {
            commit();
        }

        return result;
    }

    protected int toCount(ExecuteResult executeResult) {
        long result = 0;
        for (Long count : executeResult.getCounters().values()) {
            result += count;
        }
        return (int) result;
    }

    protected void logExceptionRecord(Exception e, List<Parameter> parameter) {
        String code;
        if (e instanceof ServerException) {
            code = ((ServerException) e).getDiagnosticCode().name();
        } else {
            code = e.getClass().getSimpleName();
        }
        logger.debug("{} occurred. message={}, parameter={}", code, e.getMessage(), parameter, e);
    }

    public int[] executeInsert(PreparedStatement ps, List<List<Parameter>> list) throws IOException, ServerException {
        var futures = new Futures(list);
        try (futures) {
            var tx = getTransaction();
            for (var parameter : list) {
                futures.add(tx.executeStatement(ps, parameter));
            }
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }

        if (autoCommit) {
            commit();
        }

        return futures.getResult();
    }

    private class Futures implements AutoCloseable {
        private final List<List<Parameter>> parameterList;
        private final List<FutureResponse<ExecuteResult>> futureList;
        private final int[] result;

        public Futures(List<List<Parameter>> list) {
            this.parameterList = list;
            int size = list.size();
            this.futureList = new ArrayList<>(size);
            this.result = new int[size];
        }

        public void add(FutureResponse<ExecuteResult> future) {
            futureList.add(future);
        }

        @Override
        public void close() throws IOException, ServerException, InterruptedException, TimeoutException {
            var exceptionSet = new HashSet<Class<?>>();
            Exception futureException = null;
            int timeout = task.getInsertTimeout();
            int i = 0;
            for (var future : futureList) {
                try {
                    var executeResult = future.await(timeout, TimeUnit.SECONDS);
                    result[i] = toCount(executeResult);
                } catch (Exception e) {
                    if (futureException == null) {
                        var parameter = parameterList.get(i);
                        logExceptionRecord(e, parameter);
                    }
                    var exClass = e.getClass();
                    if (exceptionSet.add(exClass)) {
                        if (futureException == null) {
                            futureException = e;
                        } else {
                            futureException.addSuppressed(e);
                        }
                    }
                }
                i++;
            }
            if (futureException != null) {
                if (futureException instanceof IOException) {
                    throw (IOException) futureException;
                }
                if (futureException instanceof ServerException) {
                    throw (ServerException) futureException;
                }
                if (futureException instanceof InterruptedException) {
                    throw (InterruptedException) futureException;
                }
                if (futureException instanceof TimeoutException) {
                    throw (TimeoutException) futureException;
                }
                if (futureException instanceof RuntimeException) {
                    throw (RuntimeException) futureException;
                }
                throw new RuntimeException(futureException);
            }
        }

        public int[] getResult() {
            return this.result;
        }
    }

    public int[] executeBatch(PreparedStatement ps, List<List<Parameter>> list) throws IOException, ServerException {
        var tx = getTransaction();
        try {
            int timeout = task.getInsertTimeout();
            tx.batch(ps, list).await(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }

        if (autoCommit) {
            commit();
        }

        // TODO Tsurugi get count
        var result = new int[list.size()];
        Arrays.fill(result, 1);
        return result;
    }

    public boolean existsTransaction() {
        return transaction != null;
    }

    public void commitIfNecessary() throws ServerException {
        if (!autoCommit) {
            try {
                commit();
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }
    }

    public ServerException safeRollback(ServerException cause) {
        try {
            if (!autoCommit) {
                rollback();
            }
            return cause;
        } catch (ServerException ex) {
            if (cause != null) {
                cause.addSuppressed(ex);
                return cause;
            }
            return ex;
        }
    }

    public synchronized void commit() throws IOException, ServerException {
        if (transaction == null) {
            return;
        }

        try {
            int timeout = task.getCommitTimeout();
            transaction.commit(commitStatus).await(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }

        transactionClose();
    }

    public synchronized void rollback() throws ServerException {
        if (transaction == null) {
            return;
        }

        try {
            int timeout = task.getCommitTimeout();
            transaction.rollback().await(timeout, TimeUnit.SECONDS);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }

        transactionClose();
    }

    private void transactionClose() throws ServerException {
        try {
            transaction.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        transaction = null;
    }

    @Override
    public void close() throws ServerException {
        try (sqlClient; var t = transaction) {
            // close only
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
