package storage.writer;

import storage.file.*;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class MetaFileWriter extends SegmentFileWriter {
    private final SegmentMetadata metadata;

    public MetaFileWriter(Path path, int bufferSize, SegmentMetadata metadata) throws IOException {
        super(path, bufferSize);
        this.metadata = metadata;
        
        if (metadata == null) {
            throw new IllegalArgumentException("Metadata cannot be null");
        }

        validateMetadata(metadata);

        FeatherFileHeader header = new FeatherFileHeader(FileType.META, metadata.getDocumentCount());
        writeHeader(header);

        writeMetadata();
    }

    private void validateMetadata(SegmentMetadata metadata) {
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

    private void writeMetadata() throws IOException {
        position = FeatherFileHeader.HEADER_SIZE;
        metadata.writeTo(channel, position);
    }

    @Override
    public MetaFile complete() throws IOException {
        // Create and return the read-only file
        close();

        FileChannel readChannel = FileChannel.open(path, StandardOpenOption.READ);
        return new MetaFile(readChannel, bufferSize);
    }
} 