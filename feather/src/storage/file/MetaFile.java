package storage.file;

import java.io.IOException;
import java.nio.channels.FileChannel;

public class MetaFile extends SegmentFile {
    private final SegmentMetadata metadata;

    public MetaFile(FileChannel channel, int bufferSize, SegmentMetadata metadata) throws IOException {
        super(channel, bufferSize);
        this.metadata = metadata;
        writeMetadata();
    }

    public MetaFile(FileChannel channel, int bufferSize) throws IOException {
        super(channel, bufferSize);
        this.metadata = readMetadata();
    }

    public MetaFile(FileChannel channel, int bufferSize, FeatherFileHeader header) throws IOException {
        super(channel, bufferSize, header);
        this.metadata = readMetadata();
    }

    public MetaFile(FileChannel channel, int bufferSize, FeatherFileHeader header, SegmentMetadata metadata) throws IOException {
        super(channel, bufferSize, header);
        this.metadata = metadata;
    }

    @Override
    protected FileType getFileType() {
        return FileType.META;
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
