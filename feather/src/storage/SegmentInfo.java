package storage;

import java.io.Serializable;
import java.util.Objects;

public class SegmentInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final String name;
    private final long creationTime;
    
    private final int documentCount;
    private final int minDocId;
    private final int maxDocId;
    
    private boolean deleted;
    
    private long sizeInBytes = -1;
    
    public SegmentInfo(String name, long creationTime, int documentCount, 
                      int minDocId, int maxDocId) {
        this(name, creationTime, documentCount, minDocId, maxDocId, false);
    }
    
    public SegmentInfo(String name, long creationTime, int documentCount, 
                      int minDocId, int maxDocId, boolean deleted) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Segment name cannot be null or empty");
        }
        if (documentCount < 0) {
            throw new IllegalArgumentException("Document count cannot be negative");
        }
        if (minDocId > maxDocId) {
            throw new IllegalArgumentException("minDocId cannot be greater than maxDocId");
        }
        
        this.name = name;
        this.creationTime = creationTime;
        this.documentCount = documentCount;
        this.minDocId = minDocId;
        this.maxDocId = maxDocId;
        this.deleted = deleted;
    }
    
    public String getName() {
        return name;
    }
    
    public long getCreationTime() {
        return creationTime;
    }
    
    public int getDocumentCount() {
        return documentCount;
    }
    
    public int getMinDocId() {
        return minDocId;
    }
    
    public int getMaxDocId() {
        return maxDocId;
    }
    
    public boolean isDeleted() {
        return deleted;
    }
    
    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }
    
    public long getSizeInBytes() {
        return sizeInBytes;
    }
    
    public void setSizeInBytes(long sizeInBytes) {
        if (sizeInBytes < 0 && sizeInBytes != -1) {
            throw new IllegalArgumentException("Size cannot be negative (except -1 for unknown)");
        }
        this.sizeInBytes = sizeInBytes;
    }
    
    public boolean containsDocId(int docId) {
        return docId >= minDocId && docId <= maxDocId;
    }
    
    public int size() {
        return documentCount;
    }
    
    public int getDocIdRange() {
        return maxDocId - minDocId + 1;
    }
    
    @Override
    public String toString() {
        return "SegmentInfo{" +
                "name='" + name + '\'' +
                ", docs=" + documentCount +
                ", docIds=[" + minDocId + "-" + maxDocId + "]" +
                (deleted ? ", DELETED" : "") +
                (sizeInBytes != -1 ? ", size=" + sizeInBytes + "B" : "") +
                '}';
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SegmentInfo that = (SegmentInfo) o;
        return creationTime == that.creationTime &&
                documentCount == that.documentCount &&
                minDocId == that.minDocId &&
                maxDocId == that.maxDocId &&
                deleted == that.deleted &&
                Objects.equals(name, that.name);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(name, creationTime, documentCount, minDocId, maxDocId, deleted);
    }
}