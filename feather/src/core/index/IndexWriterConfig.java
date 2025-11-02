package core.index;

import core.analysis.FeatherAnalyzer;
import storage.merge.MergePolicy;

import java.util.Objects;

public class IndexWriterConfig {
    private final FeatherAnalyzer analyzer;
    private final MergePolicy mergePolicy;
    private final int maxBufferedDocs;

    public IndexWriterConfig(FeatherAnalyzer analyzer, MergePolicy mergePolicy, int maxBufferedDocs) {
        this.analyzer = Objects.requireNonNull(analyzer, "FeatherAnalyzer must not be null");
        this.mergePolicy = Objects.requireNonNull(mergePolicy, "MergePolicy must not be null");
        if (maxBufferedDocs <= 0) {
            throw new IllegalArgumentException("maxBufferedDocs must be greater than 0");
        }
        this.maxBufferedDocs = maxBufferedDocs;
    }

    public FeatherAnalyzer getAnalyzer() { return analyzer; }
    public MergePolicy getMergePolicy() { return mergePolicy; }
    public int getMaxBufferedDocs() { return maxBufferedDocs; }
}