package storage.file;

import storage.exception.InvalidHeaderException;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public abstract class SegmentFile implements Closeable {

    private volatile boolean closed = false;

    protected final FileChannel channel;
    protected final FeatherFileHeader header;
    protected final ByteBuffer buffer;
    protected long position;

    /*
        TODO: get the buffer size from Lucene-like IOContext class when created
     */
    protected SegmentFile(FileChannel channel, int bufferSize)
            throws IOException {
        this.channel = channel;
        this.buffer = ByteBuffer.allocate(bufferSize);
        this.header = readHeader();
        this.position = FeatherFileHeader.HEADER_SIZE;
        validateFileType();
    }

    protected SegmentFile(FileChannel channel, int bufferSize, FeatherFileHeader header)
            throws IOException {
        this.channel = channel;
        this.buffer = ByteBuffer.allocate(bufferSize);

        if (header != null) {
            this.header = header;
            header.writeTo(channel);
        } else if (channel.size() == 0) {
            this.header = new FeatherFileHeader(
                    getFileType(),
                    0
            );
            this.header.writeTo(channel);
        } else {
            this.header = readHeader();
        }

        this.position = FeatherFileHeader.HEADER_SIZE;
        validateFileType();
    }

    private FeatherFileHeader readHeader() throws IOException {
        if (channel.size() < FeatherFileHeader.HEADER_SIZE) {
            throw new InvalidHeaderException(
                    "File too small to contain header");
        }

        channel.position(0);
        return FeatherFileHeader.readFrom(channel);
    }

    // TODO: write a method ensuring status to be opened

    protected void validateFileType() throws IOException {
        FileType expected = getFileType();
        FileType actual = header.getFileType();

        if (actual != expected) {
            throw new InvalidHeaderException(String.format(
                    "Invalid file type: expected %s but was %s",
                    expected, actual));
        }
    }

    protected abstract FileType getFileType();

    public void flush() throws IOException {
        channel.force(false);
    }

    public long size() throws IOException {
        return channel.size();
    }

    protected int readInt() throws IOException {
        buffer.clear().limit(4);
        channel.read(buffer, position);
        buffer.flip();
        position += 4;
        return buffer.getInt();
    }

    protected void writeInt(int value) throws IOException {
        buffer.clear();
        buffer.putInt(value);
        buffer.flip();
        channel.write(buffer, position);
        position += 4;
    }

    protected void writeLong(long value) throws IOException {
        buffer.clear();
        buffer.putLong(value);
        buffer.flip();
        channel.write(buffer, position);
        position += 8;
    }

    protected long readLong() throws IOException {
        buffer.clear().limit(8);
        channel.read(buffer, position);
        buffer.flip();
        position += 8;
        return buffer.getLong();
    }

    protected short readShort() throws IOException {
        buffer.clear().limit(2);
        channel.read(buffer, position);
        buffer.flip();
        position += 2;
        return buffer.getShort();
    }

    protected void writeShort(short value) throws IOException {
        buffer.clear();
        buffer.putShort(value);
        buffer.flip();
        channel.write(buffer, position);
        position += 2;
    }

    protected ByteBuffer readBytes(int length) throws IOException {
        ByteBuffer data = ByteBuffer.allocate(length);
        channel.read(data, position);
        position += length;
        data.flip();
        return data;
    }

    protected void writeBytes(ByteBuffer data) throws IOException {
        data.position(0);
        int length = data.remaining();
        channel.write(data, position);
        position += length;
    }

    protected void seek(long newPosition) throws IOException {
        this.position = newPosition;
        buffer.clear();
    }

    public void seekToContent() throws IOException {
        seek(FeatherFileHeader.HEADER_SIZE);
    }

    public long getPosition() {
        return position;
    }

    public FileType getHeaderFileType() {
        return header.getFileType();
    }

    public int getHeaderRecordCount() {
        return header.getRecordCount();
    }

    // TODO: need a protected method for segment merge

    @Override
    public void close() throws IOException {
        if (!closed) {
            try {
                flush();
                channel.close();
            } finally {
                closed = true;
            }
        }
    }
}
