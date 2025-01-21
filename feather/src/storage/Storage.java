package storage;

import storage.file.FileType;
import storage.file.SegmentFile;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

public abstract class Storage implements Closeable {
    protected final Path rootPath;
    private volatile boolean closed = false;

    protected Storage(Path rootPath) {
        this.rootPath = rootPath;
    }

    protected final void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Storage is closed");
        }
    }

    // segment files should not be modified
    public abstract SegmentFile createFile(String name, FileType type)
            throws IOException;
    public abstract SegmentFile openFile(String name) throws IOException;
    public abstract void deleteFile(String name) throws IOException;
    public abstract boolean exists(String name);

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

    protected void closeInternal() throws IOException {
        // 하위 클래스에서 구현
    }
}