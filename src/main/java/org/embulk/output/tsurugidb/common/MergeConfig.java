package org.embulk.output.tsurugidb.common;

import java.util.List;
import java.util.Optional;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/MergeConfig.java
public class MergeConfig {
    private final List<String> mergeKeys;
    private final Optional<List<String>> mergeRule;

    public MergeConfig(List<String> mergeKeys, Optional<List<String>> mergeRule) {
        this.mergeKeys = mergeKeys;
        this.mergeRule = mergeRule;
    }

    public List<String> getMergeKeys() {
        return mergeKeys;
    }

    public Optional<List<String>> getMergeRule() {
        return mergeRule;
    }
}
