package storage.writer;

import storage.file.FeatherFileHeader;
import storage.file.SegmentFile;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public abstract class SegmentFileWriter implements Closeable {
    protected final FileChannel channel;
    protected final ByteBuffer buffer;
    protected long position;
    protected int bufferSize = 8192;
    protected final Path path;

    protected SegmentFileWriter(Path path, int bufferSize) throws IOException {
        this.channel = FileChannel.open(path,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE);
        this.buffer = ByteBuffer.allocate(bufferSize);
        this.path = path;
        this.position = 0;
    }

    protected void writeHeader(FeatherFileHeader header) throws IOException {
        header.writeTo(channel);
        position = FeatherFileHeader.HEADER_SIZE;
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

    protected void writeShort(short value) throws IOException {
        buffer.clear();
        buffer.putShort(value);
        buffer.flip();
        channel.write(buffer, position);
        position += 2;
    }

    protected void writeBytes(ByteBuffer data) throws IOException {
        data.position(0);
        int length = data.remaining();
        channel.write(data, position);
        position += length;
    }

    public abstract SegmentFile complete() throws IOException;

    protected void flush() throws IOException {
        channel.force(false);
    }

    @Override
    public void close() throws IOException {
        flush();
        channel.close();
    }
}
