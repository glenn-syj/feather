package storage;

import storage.file.FileType;
import storage.file.SegmentFile;

import java.io.Closeable;
import java.io.IOException;

public abstract class Storage implements Closeable {
    private volatile boolean closed = false;

    protected final void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Storage is closed");
        }
    }

    // file operations
    public abstract SegmentFile createFile(String name, FileType type) throws IOException;
    public abstract SegmentFile openFile(String name) throws IOException;
    public abstract void deleteFile(String name) throws IOException;
    public abstract String[] listFiles() throws IOException;

    @Override
    public void close() throws IOException {
        if (!closed) {
            try {
                closeInternal();
            } finally {
                closed = true;
            }
        }
    }

    protected abstract void closeInternal() throws IOException;
}