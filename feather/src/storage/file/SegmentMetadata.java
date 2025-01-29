package storage.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.zip.CRC32;

public class SegmentMetadata {
    private final long creationTime;
    private final int documentCount;
    private final int minDocId;
    private final int maxDocId;
    private final long checksum;

    public SegmentMetadata(int documentCount, int minDocId, int maxDocId) {
        this.creationTime = System.currentTimeMillis();
        this.documentCount = documentCount;
        this.minDocId = minDocId;
        this.maxDocId = maxDocId;
        this.checksum = calculateChecksum();
    }

    private SegmentMetadata(long creationTime, int documentCount,
                            int minDocId, int maxDocId, long checksum) {
        this.creationTime = creationTime;
        this.documentCount = documentCount;
        this.minDocId = minDocId;
        this.maxDocId = maxDocId;
        this.checksum = checksum;

        long calculatedChecksum = calculateChecksum();
        if (calculatedChecksum != checksum) {
            throw new IllegalStateException("Metadata checksum mismatch");
        }
    }

    public static SegmentMetadata read(FileChannel channel, long position) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(32);  // 8 + 4 + 4 + 4 + 8 = 28 bytes

        channel.read(buffer, position);
        buffer.flip();

        long creationTime = buffer.getLong();
        int documentCount = buffer.getInt();
        int minDocId = buffer.getInt();
        int maxDocId = buffer.getInt();
        long checksum = buffer.getLong();

        return new SegmentMetadata(creationTime, documentCount, minDocId, maxDocId, checksum);
    }

    public void writeTo(FileChannel channel, long position) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(32);

        buffer.putLong(creationTime);
        buffer.putInt(documentCount);
        buffer.putInt(minDocId);
        buffer.putInt(maxDocId);
        buffer.putLong(checksum);

        buffer.flip();
        channel.write(buffer, position);
    }

    private long calculateChecksum() {
        CRC32 crc32 = new CRC32();
        crc32.update(Long.hashCode(creationTime));
        crc32.update(documentCount);
        crc32.update(minDocId);
        crc32.update(maxDocId);
        return crc32.getValue();
    }

    public long getCreationTime() { return creationTime; }
    public int getDocumentCount() { return documentCount; }
    public int getMinDocId() { return minDocId; }
    public int getMaxDocId() { return maxDocId; }
    public long getChecksum() { return checksum; }

    @Override
    public String toString() {
        return String.format(
                "SegmentMetadata{documents=%d, docIds=[%d,%d], created=%d}",
                documentCount, minDocId, maxDocId, creationTime
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SegmentMetadata that = (SegmentMetadata) o;
        return creationTime == that.creationTime &&
                documentCount == that.documentCount &&
                minDocId == that.minDocId &&
                maxDocId == that.maxDocId &&
                checksum == that.checksum;
    }

    @Override
    public int hashCode() {
        return Objects.hash(creationTime, documentCount, minDocId, maxDocId, checksum);
    }
}
