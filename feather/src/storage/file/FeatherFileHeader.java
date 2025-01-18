package storage.file;

import storage.exception.InvalidHeaderException;

import java.io.*;
import java.nio.channels.Channels;
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
        if (fileType == null) {
            throw new IllegalArgumentException("FileType cannot be null");
        }
        if (recordCount < 0) {
            throw new IllegalArgumentException("Record count cannot be negative");
        }
        this.fileType = fileType;
        this.recordCount = recordCount;
        this.timestamp = System.currentTimeMillis();
    }

    public void writeTo(DataOutput out) throws IOException {
        out.writeInt(MAGIC_NUMBER);
        out.writeInt(VERSION);
        out.writeByte(fileType.getCode());
        out.writeInt(recordCount);
        out.writeLong(timestamp);
        out.writeInt(HEADER_SIZE);
    }

    public void writeTo(FileChannel channel) throws IOException {
        DataOutputStream dos = new DataOutputStream(
                Channels.newOutputStream(channel));
        writeTo(dos);
        dos.flush();
    }

    public static FeatherFileHeader readFrom(DataInput in) throws IOException {
        int magic = in.readInt();
        if (magic != MAGIC_NUMBER) {
            throw new InvalidHeaderException(
                    String.format("Invalid magic number: 0x%08X", magic));
        }

        int version = in.readInt();
        if (version != VERSION) {
            throw new InvalidHeaderException(
                    String.format("Unsupported version: 0x%08X", version));
        }

        byte typeCode = in.readByte();
        FileType fileType = FileType.fromCode(typeCode);

        int recordCount = in.readInt();
        long timestamp = in.readLong();
        int headerSize = in.readInt();

        if (headerSize != HEADER_SIZE) {
            throw new InvalidHeaderException(
                    "Invalid header size: " + headerSize);
        }

        return new FeatherFileHeader(fileType, recordCount);
    }

    public static FeatherFileHeader readFrom(FileChannel channel)
            throws IOException {
        DataInputStream dis = new DataInputStream(
                Channels.newInputStream(channel));

        try {
            return readFrom(dis);
        } catch (EOFException e) {
            throw new EOFException("Reached end of file while reading header");
        }
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