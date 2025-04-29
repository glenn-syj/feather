package storage.merge;

import storage.file.SegmentMetadata;
import java.util.List;
import java.util.Collections;

public class MergeSpec {
    private final List<SegmentMetadata> segments;
    private final String mergedName;

    public MergeSpec(List<SegmentMetadata> segments) {
        if (segments == null || segments.size() < 2) {
            throw new IllegalArgumentException("Must provide at least 2 segments to merge");
        }

        this.segments = List.of(segments.toArray(SegmentMetadata[]::new));
        this.mergedName = generateMergedName(segments);
    }

    private String generateMergedName(List<SegmentMetadata> segments) {
        return String.format("m_%d_%d",
                segments.size(),
                System.currentTimeMillis());
    }

    public List<SegmentMetadata> getSegments() {
        return segments;
    }

    public String getMergedName() {
        return mergedName;
    }
}
