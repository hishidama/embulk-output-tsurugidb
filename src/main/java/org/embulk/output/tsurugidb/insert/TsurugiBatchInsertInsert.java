package org.embulk.output.tsurugidb.insert;

import java.io.IOException;
import java.util.Optional;

import org.embulk.output.tsurugidb.TsurugiOutputConnector;
import org.embulk.output.tsurugidb.TsurugiOutputPlugin.PluginTask;
import org.embulk.output.tsurugidb.common.MergeConfig;

import com.tsurugidb.tsubakuro.exception.ServerException;

public class TsurugiBatchInsertInsert extends TsurugiSqlBatchInsert {

    public TsurugiBatchInsertInsert(PluginTask task, TsurugiOutputConnector connector, Optional<MergeConfig> mergeConfig) {
        super(task, connector, mergeConfig);
    }

    @Override
    protected int[] executeBatch() throws IOException, ServerException {
        return executor.executeInsert(batch, recordList);
    }
}
