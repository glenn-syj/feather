package storage.file;

import storage.exception.InvalidHeaderException;

import java.io.IOException;
import java.nio.channels.FileChannel;

public class MetaFile extends SegmentFile {
    private final SegmentMetadata metadata;

    /*
        TODO: need to refactor the validation code based on validator pattern
     */

    public MetaFile(FileChannel channel, int bufferSize, SegmentMetadata metadata) throws IOException {
        super(channel, bufferSize);
        this.metadata = metadata;
        writeMetadata();
        validateMetadata(metadata);
        validateHeaderConsistency(header, this.metadata);
    }

    public MetaFile(FileChannel channel, int bufferSize) throws IOException {
        super(channel, bufferSize);
        this.metadata = readMetadata();
        validateMetadata(metadata);
        validateHeaderConsistency(header, this.metadata);
    }

    public MetaFile(FileChannel channel, int bufferSize, FeatherFileHeader header) throws IOException {
        super(channel, bufferSize, header);
        this.metadata = readMetadata();
        validateMetadata(metadata);
        validateHeaderConsistency(header, this.metadata);
    }

    public MetaFile(FileChannel channel, int bufferSize, FeatherFileHeader header, SegmentMetadata metadata) throws IOException {
        super(channel, bufferSize, header);
        this.metadata = metadata;
        validateMetadata(metadata);
        validateHeaderConsistency(header, this.metadata);
    }

    @Override
    protected FileType getFileType() {
        return FileType.META;
    }

    private void validateMetadata(SegmentMetadata metadata) {
        if (metadata == null) {
            throw new IllegalArgumentException("Metadata cannot be null");
        }

        if (metadata.getDocumentCount() < 0) {
            throw new IllegalArgumentException(
                    "Document count cannot be negative: " + metadata.getDocumentCount());
        }

        if (metadata.getMinDocId() > metadata.getMaxDocId()) {
            throw new IllegalArgumentException(String.format(
                    "Invalid document ID range: [%d,%d]",
                    metadata.getMinDocId(), metadata.getMaxDocId()));
        }

        if (metadata.getDocumentCount() > 0 && metadata.getMinDocId() < 0) {
            throw new IllegalArgumentException(
                    "Document IDs cannot be negative when document count > 0");
        }

        int idRange = metadata.getMaxDocId() - metadata.getMinDocId() + 1;
        if (metadata.getDocumentCount() > idRange) {
            throw new IllegalArgumentException(String.format(
                    "Document count (%d) exceeds ID range [%d,%d]",
                    metadata.getDocumentCount(), metadata.getMinDocId(), metadata.getMaxDocId()));
        }
    }

    private void validateHeaderConsistency(FeatherFileHeader header, SegmentMetadata metadata) {
        if (header.getFileType() != FileType.META) {
            throw new InvalidHeaderException(String.format(
                    "Invalid file type: expected %s but was %s",
                    FileType.META, header.getFileType()));
        }

        if (header.getRecordCount() != metadata.getDocumentCount()) {
            throw new IllegalArgumentException(String.format(
                    "Header record count (%d) does not match metadata document count (%d)",
                    header.getRecordCount(), metadata.getDocumentCount()));
        }
    }

    private void writeMetadata() throws IOException {
        long currentPosition = channel.position();
        channel.position(FeatherFileHeader.HEADER_SIZE);

        metadata.writeTo(channel, channel.position());

        channel.position(currentPosition);

        flush();
    }

    private SegmentMetadata readMetadata() throws IOException {
        long currentPosition = channel.position();

        channel.position(FeatherFileHeader.HEADER_SIZE);

        SegmentMetadata metadata = SegmentMetadata.read(channel, channel.position());

        channel.position(currentPosition);

        return metadata;
    }

    public long getCreationTime() { return metadata.getCreationTime(); }
    public int getDocumentCount() { return metadata.getDocumentCount(); }
    public int getMinDocId() { return metadata.getMinDocId(); }
    public int getMaxDocId() { return metadata.getMaxDocId(); }

    @Override
    public String toString() {
        return String.format("MetaFile{%s}", metadata);
    }
}
