package storage.merge;

import storage.file.SegmentMetadata;
import java.util.List;

public abstract class MergePolicy {
    // default config values

    protected static final int DEFAULT_MAX_MERGE_AT_ONCE = 10;
    protected static final int MIN_SEGMENT_DOCS = 1000;
    protected static final int MAX_SEGMENT_DOCS = 1_000_000;

    // a limitation for segments to get merged at a time
    private final int maxMergeAtOnce;
    private final int maxSegmentDocs;

    protected MergePolicy() {
        this(DEFAULT_MAX_MERGE_AT_ONCE, MAX_SEGMENT_DOCS);
    }

    protected MergePolicy(int maxMergeAtOnce, int maxSegmentDocs) {
        validateSettings(maxMergeAtOnce, maxSegmentDocs);
        this.maxMergeAtOnce = maxMergeAtOnce;
        this.maxSegmentDocs = maxSegmentDocs;
    }

    private void validateSettings(int maxMergeAtOnce, int maxSegmentDocs) {
        if (maxMergeAtOnce < 2) {
            throw new IllegalArgumentException("maxMergeAtOnce must be >= 2");
        }
        if (maxSegmentDocs <= MIN_SEGMENT_DOCS) {
            throw new IllegalArgumentException(
                    "maxSegmentDocs must be > " + MIN_SEGMENT_DOCS);
        }
    }

    public abstract MergeSpec findMerges(List<SegmentMetadata> segments);

    protected boolean exceedsMaxDocs(List<SegmentMetadata> segments) {
        return segments.stream()
                .mapToInt(SegmentMetadata::getDocumentCount)
                .sum() > maxSegmentDocs;
    }

    // Getters
    protected int getMaxMergeAtOnce() { return maxMergeAtOnce; }
    protected int getMaxSegmentDocs() { return maxSegmentDocs; }
}