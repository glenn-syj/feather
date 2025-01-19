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

    protected void flush() throws IOException {
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

    protected ByteBuffer readBytes(int length) throws IOException {
        ByteBuffer data = ByteBuffer.allocate(length);
        channel.read(data, position);
        position += length;
        data.flip();
        return data;
    }

    protected void writeBytes(ByteBuffer data) throws IOException {
        channel.write(data, position);
        position += data.remaining();
    }

    protected void seek(long newPosition) throws IOException {
        this.position = newPosition;
        buffer.clear();
    }

    public long getPosition() {
        return position;
    }

    protected void updateRecordCount(int newCount) throws IOException {
        long savedPosition = position;

        // move to recordCounter field
        // (magic + version + fileType = 9 bytes)
        seek(9);
        writeInt(newCount);

        seek(savedPosition);
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
