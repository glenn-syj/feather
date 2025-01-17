package storage.file;

import storage.exception.InvalidHeaderException;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Represents the header structure for all Feather segment files.
 * The header contains metadata about the file including its type, version,
 * and record information.
 */
public final class FeatherFileHeader {
    public static final int MAGIC_NUMBER = 0x46544852;  // "FTHR"
    public static final int VERSION = 0x00010000;       // 1.0

    public static final int HEADER_SIZE =
            4 +  // magic number
                    4 +  // version
                    1 +  // file type
                    4 +  // record count
                    8 +  // timestamp
                    4;   // header size

    private final FileType fileType;
    private final int recordCount;
    private final long timestamp;

    public FeatherFileHeader(FileType fileType, int recordCount) {
        this.fileType = fileType;
        this.recordCount = recordCount;
        this.timestamp = System.currentTimeMillis();
    }

    public ByteBuffer write() {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
        buffer.putInt(MAGIC_NUMBER)
                .putInt(VERSION)
                .put(fileType.getCode())
                .putInt(recordCount)
                .putLong(timestamp)
                .putInt(HEADER_SIZE);
        buffer.flip();
        return buffer;
    }

    public void writeTo(FileChannel channel) throws IOException {
        ByteBuffer buffer = write();
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    public static FeatherFileHeader read(ByteBuffer buffer) {
        if (buffer.remaining() < HEADER_SIZE) {
            throw new InvalidHeaderException("Buffer too small for header");
        }

        int magic = buffer.getInt();
        if (magic != MAGIC_NUMBER) {
            throw new InvalidHeaderException("Invalid magic number: " +
                    String.format("0x%08X", magic));
        }

        int version = buffer.getInt();
        if (version != VERSION) {
            throw new InvalidHeaderException("Unsupported version: " +
                    String.format("0x%08X", version));
        }

        byte typeCode = buffer.get();
        FileType fileType = FileType.fromCode(typeCode);

        int recordCount = buffer.getInt();
        long timestamp = buffer.getLong();
        int headerSize = buffer.getInt();

        if (headerSize != HEADER_SIZE) {
            throw new InvalidHeaderException("Invalid header size: " + headerSize);
        }

        return new FeatherFileHeader(fileType, recordCount);
    }

    public static FeatherFileHeader readFrom(FileChannel channel)
            throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
        while (buffer.hasRemaining()) {
            if (channel.read(buffer) == -1) {
                throw new EOFException("Reached end of file while reading header");
            }
        }
        buffer.flip();
        return read(buffer);
    }

    public FileType getFileType() { return fileType; }
    public int getRecordCount() { return recordCount; }
    public long getTimestamp() { return timestamp; }
    public int getHeaderSize() { return HEADER_SIZE; }

    @Override
    public String toString() {
        return String.format(
                "FeatherFileHeader{type=%s, records=%d, timestamp=%d, size=%d}",
                fileType, recordCount, timestamp, HEADER_SIZE
        );
    }
}