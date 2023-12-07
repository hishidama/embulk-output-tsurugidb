package org.embulk.output.tsurugidb.executor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.embulk.config.ConfigException;
import org.embulk.output.tsurugidb.TsurugiOutputPlugin.PluginTask;
import org.embulk.output.tsurugidb.insert.InsertMethodSub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.kvs.proto.KvsTransaction.Priority;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.BatchScript;
import com.tsurugidb.tsubakuro.kvs.BatchScript.Ref;
import com.tsurugidb.tsubakuro.kvs.CommitType;
import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.PutResult;
import com.tsurugidb.tsubakuro.kvs.PutType;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.kvs.TransactionHandle;
import com.tsurugidb.tsubakuro.kvs.TransactionOption;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.Timeout;
import com.tsurugidb.tsubakuro.util.Timeout.Policy;

public class TsurugiKvsExecutor implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(TsurugiKvsExecutor.class);

    public static TsurugiKvsExecutor create(PluginTask task, Session session, boolean autoCommit) {
        var txOption = getTxOption(task);
        logger.debug("txOption={}", txOption);
        var commitType = task.getCommitType().toKvsCommitType();
        logger.debug("commitType={}", commitType);

        return new TsurugiKvsExecutor(task, session, txOption, commitType, autoCommit);
    }

    private static TransactionOption getTxOption(PluginTask task) {
        TransactionOption.Builder builder;

        String txType = task.getTxType();
        switch (txType.toUpperCase()) {
        case "OCC":
            builder = TransactionOption.forShortTransaction();
            break;
        case "LTX":
            var ltxBuilder = TransactionOption.forLongTransaction();
            builder = ltxBuilder;
            String table = task.getTable();
            ltxBuilder.addWritePreserve(table);
            ltxBuilder.addInclusiveReadArea(table);
            task.getTxWritePreserve().forEach(tableName -> {
                ltxBuilder.addWritePreserve(tableName);
            });
            task.getTxInclusiveReadArea().forEach(tableName -> {
                ltxBuilder.addInclusiveReadArea(tableName);
            });
            task.getTxExclusiveReadArea().forEach(tableName -> {
                ltxBuilder.addExclusiveReadArea(tableName);
            });
            fillPriority(builder, task);
            break;
        case "RTX":
            var rtxBuilder = TransactionOption.forReadOnlyTransaction();
            builder = rtxBuilder;
            fillPriority(builder, task);
            break;
        default:
            throw new ConfigException("unsupported tx_type(" + txType + "). choose from OCC,LTX,RTX");
        }

//TODO Tsurugi        builder.withLabel(task.getTxLabel());
        return builder.build();
    }

    private static void fillPriority(TransactionOption.Builder builder, PluginTask task) {
        task.getTxPriority().ifPresent(s -> {
            Priority priority;
            try {
                priority = Priority.valueOf(s.toUpperCase());
            } catch (Exception e) {
                var ce = new ConfigException(MessageFormat.format("Unknown tx_priority ''{0}''. Supported tx_priority are {1}", //
                        s, Arrays.stream(Priority.values()).map(Priority::toString).map(String::toLowerCase).collect(Collectors.joining(", "))));
                ce.addSuppressed(e);
                throw ce;
            }
            builder.withPriority(priority);
        });
    }

    private final PluginTask task;
    private final KvsClient kvsClient;
    private final PutType putType;
    private final TransactionOption txOption;
    private final CommitType commitType;
    private final boolean autoCommit;
    private TransactionHandle transactionHandle;

    public TsurugiKvsExecutor(PluginTask task, Session session, TransactionOption txOption, CommitType commitType, boolean autoCommit) {
        this.task = task;
        this.kvsClient = KvsClient.attach(session);
        this.putType = getPutType();
        this.txOption = txOption;
        this.commitType = commitType;
        this.autoCommit = autoCommit;
    }

    private static final Map<InsertMethodSub, PutType> PUT_TYPE = Map.of( //
            InsertMethodSub.PUT_OVERWRITE, PutType.OVERWRITE, //
            InsertMethodSub.PUT_IF_ABSENT, PutType.IF_ABSENT, //
            InsertMethodSub.PUT_IF_PRESENT, PutType.IF_PRESENT);

    protected PutType getPutType() {
        var option = task.getInsertMethodSub().orElse(InsertMethodSub.PUT_OVERWRITE);
        var putType = PUT_TYPE.get(option);
        if (putType != null) {
            return putType;
        }
        var message = InsertMethodSub.getUnsupportedMessage("put", option, PUT_TYPE.keySet());
        throw new ConfigException(message);
    }

    protected synchronized TransactionHandle getTransactionHandle() throws ServerException {
        if (transactionHandle == null) {
            int timeout = task.getBeginTimeout();
            try {
                transactionHandle = kvsClient.beginTransaction(txOption).await(timeout, TimeUnit.SECONDS);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            } catch (InterruptedException | TimeoutException e) {
                throw new RuntimeException(e);
            }
            transactionHandle.setCloseTimeout(new Timeout(timeout, TimeUnit.SECONDS, Policy.ERROR));
        }
        return transactionHandle;
    }

    public int[] executePutWait(String tableName, List<RecordBuffer> list) throws IOException, ServerException {
        var result = new int[list.size()];

        var tx = getTransactionHandle();
        int i = 0;
        for (var record : list) {
            try {
                int timeout = task.getInsertTimeout();
                var putResult = kvsClient.put(tx, tableName, record, putType).await(timeout, TimeUnit.SECONDS);
                result[i++] = putResult.size();
            } catch (InterruptedException | TimeoutException e) {
                logExceptionRecord(e, record);
                throw new RuntimeException(e);
            } catch (Exception e) {
                logExceptionRecord(e, record);
                throw e;
            }
        }

        if (autoCommit) {
            commit();
        }

        return result;
    }

    protected void logExceptionRecord(Exception e, RecordBuffer record) {
        String code;
        if (e instanceof ServerException) {
            code = ((ServerException) e).getDiagnosticCode().name();
        } else {
            code = e.getClass().getSimpleName();
        }
        logger.debug("{} occurred. message={}, record={}", code, e.getMessage(), record, e);
    }

    public int[] executePut(String tableName, List<RecordBuffer> list) throws IOException, ServerException {
        var futures = new Futures(list);
        try (futures) {
            var tx = getTransactionHandle();
            for (var record : list) {
                futures.add(kvsClient.put(tx, tableName, record, putType));
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
        private final List<RecordBuffer> recordList;
        private final List<FutureResponse<PutResult>> futureList;
        private final int[] result;

        public Futures(List<RecordBuffer> list) {
            this.recordList = list;
            int size = list.size();
            this.futureList = new ArrayList<>(size);
            this.result = new int[size];
        }

        public void add(FutureResponse<PutResult> futureResponse) {
            futureList.add(futureResponse);
        }

        @Override
        public void close() throws IOException, ServerException, InterruptedException, TimeoutException {
            var exceptionSet = new HashSet<Class<?>>();
            Exception futureException = null;
            int timeout = task.getInsertTimeout();
            int i = 0;
            for (var future : futureList) {
                try {
                    var putResult = future.await(timeout, TimeUnit.SECONDS);
                    result[i] = putResult.size();
                } catch (Exception e) {
                    if (futureException == null) {
                        var record = recordList.get(i);
                        logExceptionRecord(e, record);
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

    public int[] executeBatch(String tableName, List<RecordBuffer> list) throws IOException, ServerException {
        var script = new BatchScript();
        var refList = new ArrayList<Ref<PutResult>>();
        for (var record : list) {
            refList.add(script.addPut(tableName, record, putType));
        }

        var result = new int[list.size()];

        var tx = getTransactionHandle();
        try {
            int timeout = task.getInsertTimeout();
            var batchResult = kvsClient.batch(tx, script).await(timeout, TimeUnit.SECONDS);
            int i = 0;
            for (var ref : refList) {
                var putResult = batchResult.get(ref);
                result[i++] = putResult.size();
            }
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }

        if (autoCommit) {
            commit();
        }

        return result;
    }

    public boolean existsTransaction() {
        return transactionHandle != null;
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
        if (transactionHandle == null) {
            return;
        }

        try {
            int timeout = task.getCommitTimeout();
            kvsClient.commit(transactionHandle, commitType).await(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }

        transactionClose();
    }

    public synchronized void rollback() throws ServerException {
        if (transactionHandle == null) {
            return;
        }

        try {
            int timeout = task.getCommitTimeout();
            kvsClient.rollback(transactionHandle).await(timeout, TimeUnit.SECONDS);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }

        transactionClose();
    }

    private void transactionClose() throws ServerException {
        try {
            transactionHandle.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        transactionHandle = null;
    }

    @Override
    public void close() throws ServerException {
        try (kvsClient; var t = transactionHandle) {
            // close only
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
