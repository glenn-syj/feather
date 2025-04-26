package storage;

import storage.file.*;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.stream.Stream;

public class FileSystemStorage extends Storage {
    private final Path rootPath;
    private static final int BUFFER_SIZE = 8192;

    public FileSystemStorage(Path rootPath) {
        this.rootPath = rootPath;
        try {
            Files.createDirectories(rootPath);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create directory: " + rootPath, e);
        }
    }

    @Override
    public SegmentFile createFile(String name, FileType type) throws IOException {
        ensureOpen();
        Path filePath = rootPath.resolve(name);
        FileChannel channel = FileChannel.open(filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);

        return createSegmentFile(channel, type);
    }

    @Override
    public SegmentFile openFile(String name) throws IOException {
        ensureOpen();
        Path filePath = rootPath.resolve(name);

        // Open file in read/write mode to prevent NonWritableChannelException
        // Note: Current SegmentFile implementation attempts to write headers when constructing
        // Future improvement: Add read-only mode support
        FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE);

        FeatherFileHeader header = FeatherFileHeader.readFrom(channel);
        return createSegmentFile(channel, header.getFileType());
    }

    @Override
    public void deleteFile(String name) throws IOException {
        ensureOpen();
        Files.deleteIfExists(rootPath.resolve(name));
    }

    @Override
    public String[] listFiles() throws IOException {
        ensureOpen();
        try (Stream<Path> paths = Files.list(rootPath)) {
            return paths.map(p -> p.getFileName().toString())
                    .toArray(String[]::new);
        }
    }

    @Override
    protected void closeInternal() throws IOException {
        // Reserved for future resource cleanup (caches, buffer pools, etc.)
    }

    private SegmentFile createSegmentFile(FileChannel channel, FileType type) throws IOException {
        FeatherFileHeader header = new FeatherFileHeader(type, 0);

        return switch (type) {
            case DOC -> new DocumentFile(channel, BUFFER_SIZE, header);
            case DIC -> new DictionaryFile(channel, BUFFER_SIZE, header);
            case POST -> new PostingFile(channel, BUFFER_SIZE, header);
            case META -> new MetaFile(channel, BUFFER_SIZE, header);
        };
    }
}
