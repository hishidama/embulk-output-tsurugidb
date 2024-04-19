package org.embulk.output.tsurugidb;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.embulk.output.tsurugidb.TsurugiOutputPlugin.PluginTask;
import org.embulk.output.tsurugidb.common.MergeConfig;
import org.embulk.output.tsurugidb.common.TableIdentifier;
import org.embulk.output.tsurugidb.executor.TsurugiKvsExecutor;
import org.embulk.output.tsurugidb.executor.TsurugiSqlExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.TableMetadata;
import com.tsurugidb.tsubakuro.sql.exception.CcException;
import com.tsurugidb.tsubakuro.util.Timeout;
import com.tsurugidb.tsubakuro.util.Timeout.Policy;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/JdbcOutputConnection.java
public class TsurugiOutputConnection implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(TsurugiOutputConnection.class);

    public static TsurugiOutputConnection newConnection(PluginTask task, boolean retryableMetadataOperation, boolean autoCommit) throws ServerException {
        String endpoint = task.getEndpoint();
        var credential = createCredential(task);
        logger.debug("endpoint={}, credential={}", endpoint, credential);
        int connectTimeout = task.getConnectTimeout();
        int closeTimeout = task.getSocketTimeout();

        Session session;
        try {
            session = SessionBuilder.connect(endpoint) //
                    .withCredential(credential) //
                    .withApplicationName("embulk-output-tsurugidb") //
                    .withLabel(task.getConnectionLabel()) //
                    .create(connectTimeout, TimeUnit.SECONDS);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
        session.setCloseTimeout(new Timeout(closeTimeout, TimeUnit.SECONDS, Policy.WARN));

        return new TsurugiOutputConnection(task, session, autoCommit);
    }

    private static Credential createCredential(PluginTask task) {
        Optional<String> user = task.getUser();
        if (user.isPresent()) {
            String password = task.getPassword().orElse(null);
            return new UsernamePasswordCredential(user.get(), password);
        }

        return NullCredential.INSTANCE;
    }

    private final PluginTask task;
    private final Session session;
    private final boolean autoCommit;
    private TsurugiSqlExecutor sqlExecutor;
    private TsurugiKvsExecutor kvsExecutor;

    public TsurugiOutputConnection(PluginTask task, Session session, boolean autoCommit) {
        this.task = task;
        this.session = session;
        this.autoCommit = autoCommit;
    }

    public String getSchemaName() {
        return null; // TODO Tsurugi getSchemaName
    }

    public Charset getTableNameCharset() {
        return StandardCharsets.UTF_8;
    }

    public synchronized TsurugiSqlExecutor getSqlExecutor() {
        if (sqlExecutor == null) {
            sqlExecutor = TsurugiSqlExecutor.create(task, session, autoCommit);
        }
        return sqlExecutor;
    }

    public synchronized TsurugiKvsExecutor getKvsExecutor() {
        if (kvsExecutor == null) {
            kvsExecutor = TsurugiKvsExecutor.create(task, session, autoCommit);
        }
        return kvsExecutor;
    }

    public Optional<TableMetadata> findTableMetadata(String tableName) throws ServerException {
        return getSqlExecutor().findTableMetadata(tableName);
    }

    public boolean tableExists(TableIdentifier table) throws ServerException {
        return tableExists(table.getTableName()); // TODO Tsurugi tableExists(TableIdentifier)
    }

    public boolean tableExists(String tableName) throws ServerException {
        return findTableMetadata(tableName).isPresent();
    }

    protected boolean supportsTableIfExistsClause() {
        return false;
    }

    public void dropTableIfExists(TableIdentifier table) throws ServerException {
        var executor = getSqlExecutor();
        try {
            doDropTableIfExists(executor, table);
        } catch (ServerException ex) {
            throw executor.safeRollback(ex);
        }
    }

    protected void doDropTableIfExists(TsurugiSqlExecutor executor, TableIdentifier table) throws ServerException {
        if (supportsTableIfExistsClause()) {
            String sql = String.format("DROP TABLE IF EXISTS %s", getSqlExecutor().quoteTableIdentifier(table));
            executor.executeUpdate(sql);
            executor.commitIfNecessary();
        } else {
            if (tableExists(table)) {
                doDropTable(executor, table);
            }
        }
    }

    public void dropTable(TableIdentifier table) throws ServerException {
        var executor = getSqlExecutor();
        try {
            doDropTable(executor, table);
        } catch (ServerException ex) {
            throw executor.safeRollback(ex);
        }
    }

    protected void doDropTable(TsurugiSqlExecutor executor, TableIdentifier table) throws ServerException {
        String sql = String.format("DROP TABLE %s", getSqlExecutor().quoteTableIdentifier(table));
        executor.executeUpdate(sql);
        executor.commitIfNecessary();
    }

    public void createTableIfNotExists(TableIdentifier table, TsurugiTableSchema schema, Optional<String> tableConstraint, Optional<String> tableOption) throws ServerException {
        if (supportsTableIfExistsClause()) {
            var executor = getSqlExecutor();
            try {
                String sql = buildCreateTableIfNotExistsSql(table, schema, tableConstraint, tableOption);
                executor.executeUpdate(sql);
                executor.commitIfNecessary();
            } catch (ServerException ex) {
                // another client might create the table at the same time.
                if (!tableExists(table)) {
                    throw executor.safeRollback(ex);
                }
            }
        } else {
            if (!tableExists(table)) {
                try {
                    createTable(table, schema, tableConstraint, tableOption);
                } catch (Exception e) {
                    // another client might create the table at the same time.
                    if (!tableExists(table)) {
                        throw e;
                    }
                }
            }
        }
    }

    protected String buildCreateTableIfNotExistsSql(TableIdentifier table, TsurugiTableSchema schema, Optional<String> tableConstraint, Optional<String> tableOption) {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE IF NOT EXISTS ");
        getSqlExecutor().quoteTableIdentifier(sb, table);
        sb.append(buildCreateTableSchemaSql(schema, tableConstraint));
        if (tableOption.isPresent()) {
            sb.append(" ");
            sb.append(tableOption.get());
        }
        return sb.toString();
    }

    public void createTable(TableIdentifier table, TsurugiTableSchema schema, Optional<String> tableConstraint, Optional<String> tableOption) throws ServerException {
        var executor = getSqlExecutor();
        try {
            String sql = buildCreateTableSql(table, schema, tableConstraint, tableOption);
            executor.executeUpdate(sql);
            executor.commitIfNecessary();
        } catch (ServerException ex) {
            throw executor.safeRollback(ex);
        }
    }

    protected String buildCreateTableSql(TableIdentifier table, TsurugiTableSchema schema, Optional<String> tableConstraint, Optional<String> tableOption) {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE ");
        getSqlExecutor().quoteTableIdentifier(sb, table);
        sb.append(buildCreateTableSchemaSql(schema, tableConstraint));
        if (tableOption.isPresent()) {
            sb.append(" ");
            sb.append(tableOption.get());
        }
        return sb.toString();
    }

    protected String buildCreateTableSchemaSql(TsurugiTableSchema schema, Optional<String> tableConstraint) {
        StringBuilder sb = new StringBuilder();

        sb.append(" (");
        for (int i = 0; i < schema.getCount(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            getSqlExecutor().quoteIdentifierString(sb, schema.getColumnName(i));
            sb.append(" ");
            String typeName = getCreateTableTypeName(schema.getColumn(i));
            sb.append(typeName);
        }
        if (tableConstraint.isPresent()) {
            sb.append(", ");
            sb.append(tableConstraint.get());
        }
        sb.append(")");

        return sb.toString();
    }

    protected String buildRenameTableSql(TableIdentifier fromTable, TableIdentifier toTable) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ");
        getSqlExecutor().quoteTableIdentifier(sb, fromTable);
        sb.append(" RENAME TO ");
        getSqlExecutor().quoteTableIdentifier(sb, toTable);
        return sb.toString();
    }

    public static enum ColumnDeclareType {
        SIMPLE, SIZE, SIZE_AND_SCALE, SIZE_AND_OPTIONAL_SCALE,
    };

    protected String getCreateTableTypeName(TsurugiColumn c) {
        if (c.getDeclaredType().isPresent()) {
            return c.getDeclaredType().get();
        } else {
            return buildColumnTypeName(c);
        }
    }

    protected String buildColumnTypeName(TsurugiColumn c) {
        String simpleTypeName = c.getSimpleTypeName();
        switch (getColumnDeclareType(simpleTypeName, c)) {
        case SIZE:
            return String.format("%s(%d)", simpleTypeName, c.getSizeTypeParameter());
        case SIZE_AND_SCALE:
            if (c.getScaleTypeParameter() < 0) {
                return String.format("%s(%d,0)", simpleTypeName, c.getSizeTypeParameter());
            } else {
                return String.format("%s(%d,%d)", simpleTypeName, c.getSizeTypeParameter(), c.getScaleTypeParameter());
            }
        case SIZE_AND_OPTIONAL_SCALE:
            if (c.getScaleTypeParameter() < 0) {
                return String.format("%s(%d)", simpleTypeName, c.getSizeTypeParameter());
            } else {
                return String.format("%s(%d,%d)", simpleTypeName, c.getSizeTypeParameter(), c.getScaleTypeParameter());
            }
        default: // SIMPLE
            return simpleTypeName;
        }
    }

    // TODO
    private static final String[] STANDARD_SIZE_TYPE_NAMES = new String[] { "CHAR", "VARCHAR", "CHAR VARYING", "CHARACTER VARYING", "LONGVARCHAR", "NCHAR", "NVARCHAR", "NCHAR VARYING",
            "NATIONAL CHAR VARYING", "NATIONAL CHARACTER VARYING", "BINARY", "VARBINARY", "BINARY VARYING", "LONGVARBINARY", "BIT", "VARBIT", "BIT VARYING", "FLOAT", // SQL standard's FLOAT[(p)]
                                                                                                                                                                      // optionally accepts precision
    };

    private static final String[] STANDARD_SIZE_AND_SCALE_TYPE_NAMES = new String[] { "DECIMAL", "NUMERIC", };

    protected ColumnDeclareType getColumnDeclareType(String convertedTypeName, TsurugiColumn col) {
        for (String x : STANDARD_SIZE_TYPE_NAMES) {
            if (x.equals(convertedTypeName)) {
                return ColumnDeclareType.SIZE;
            }
        }

        for (String x : STANDARD_SIZE_AND_SCALE_TYPE_NAMES) {
            if (x.equals(convertedTypeName)) {
                return ColumnDeclareType.SIZE_AND_SCALE;
            }
        }

        return ColumnDeclareType.SIMPLE;
    }

    protected void executeSql(String sql) throws ServerException {
        var executor = getSqlExecutor();
        try {
            executor.executeUpdate(sql);
            executor.commitIfNecessary();
        } catch (ServerException ex) {
            throw executor.safeRollback(ex);
        }
    }

    protected void collectInsert(List<TableIdentifier> fromTables, TsurugiTableSchema schema, TableIdentifier toTable, boolean truncateDestinationFirst, Optional<String> preSql,
            Optional<String> postSql) throws ServerException {
        if (fromTables.isEmpty()) {
            return;
        }

        var executor = getSqlExecutor();
        try {
            if (truncateDestinationFirst) {
                String sql = buildTruncateSql(toTable);
                executor.executeUpdate(sql);
            }

            if (preSql.isPresent()) {
                executor.executeUpdate(preSql.get());
            }

            String sql = buildCollectInsertSql(fromTables, schema, toTable);
            executor.executeUpdate(sql);

            if (postSql.isPresent()) {
                executor.executeUpdate(postSql.get());
            }

            executor.commitIfNecessary();
        } catch (ServerException ex) {
            throw executor.safeRollback(ex);
        }
    }

    protected String buildTruncateSql(TableIdentifier table) {
        StringBuilder sb = new StringBuilder();

        sb.append("DELETE FROM ");
        getSqlExecutor().quoteTableIdentifier(sb, table);

        return sb.toString();
    }

    protected String buildCollectInsertSql(List<TableIdentifier> fromTables, TsurugiTableSchema schema, TableIdentifier toTable) {
        StringBuilder sb = new StringBuilder();

        sb.append("INSERT INTO ");
        getSqlExecutor().quoteTableIdentifier(sb, toTable);
        sb.append(" (");
        for (int i = 0; i < schema.getCount(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            getSqlExecutor().quoteIdentifierString(sb, schema.getColumnName(i));
        }
        sb.append(") ");
        for (int i = 0; i < fromTables.size(); i++) {
            if (i != 0) {
                sb.append(" UNION ALL ");
            }
            sb.append("SELECT ");
            for (int j = 0; j < schema.getCount(); j++) {
                if (j != 0) {
                    sb.append(", ");
                }
                getSqlExecutor().quoteIdentifierString(sb, schema.getColumnName(j));
            }
            sb.append(" FROM ");
            getSqlExecutor().quoteTableIdentifier(sb, fromTables.get(i));
        }

        return sb.toString();
    }

    protected void collectMerge(List<TableIdentifier> fromTables, TsurugiTableSchema schema, TableIdentifier toTable, MergeConfig mergeConfig, Optional<String> preSql, Optional<String> postSql)
            throws ServerException {
        if (fromTables.isEmpty()) {
            return;
        }

        var executor = getSqlExecutor();
        try {
            if (preSql.isPresent()) {
                executor.executeUpdate(preSql.get());
            }

            String sql = buildCollectMergeSql(fromTables, schema, toTable, mergeConfig);
            executor.executeUpdate(sql);

            if (postSql.isPresent()) {
                executor.executeUpdate(postSql.get());
            }

            executor.commitIfNecessary();
        } catch (ServerException ex) {
            throw executor.safeRollback(ex);
        }
    }

    protected String buildCollectMergeSql(List<TableIdentifier> fromTables, TsurugiTableSchema schema, TableIdentifier toTable, MergeConfig mergeConfig) {
        throw new UnsupportedOperationException("not implemented");
    }

    public void replaceTable(TableIdentifier fromTable, TsurugiTableSchema schema, TableIdentifier toTable, Optional<String> postSql) throws ServerException {
        var executor = getSqlExecutor();
        try {
            dropTableIfExists(toTable);

            executor.executeUpdate(buildRenameTableSql(fromTable, toTable));

            if (postSql.isPresent()) {
                executor.executeUpdate(postSql.get());
            }

            executor.commitIfNecessary();
        } catch (ServerException ex) {
            throw executor.safeRollback(ex);
        }
    }

    public boolean isRetryableException(Exception exception) {
        var serverException = findServerException(exception);
        if (serverException != null) {
            if (serverException instanceof CcException) {
                return true;
            }
        }
        return false;
    }

    protected ServerException findServerException(Exception exception) {
        for (Throwable t = exception; t != null; t = t.getCause()) {
            if (t instanceof ServerException) {
                return (ServerException) t;
            }
        }
        return null;
    }

    @Override
    public void close() throws ServerException {
        try (session; var s = sqlExecutor; var k = kvsExecutor) {
            // close only
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
