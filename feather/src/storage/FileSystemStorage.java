package storage;

import storage.file.*;
import storage.writer.*;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.stream.Stream;

/**
 * FileSystemStorage provides file system based implementation of the Storage interface.
 * It manages segment files on disk similar to Lucene's FSDirectory.
 */
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

    /**
     * Opens a segment file for reading.
     * 
     * @param name The name of the file to open
     * @return A read-only SegmentFile instance
     * @throws IOException If an I/O error occurs
     */
    @Override
    public SegmentFile openFile(String name) throws IOException {
        ensureOpen();
        Path filePath = rootPath.resolve(name);
        
        // Open file in read-only mode since SegmentFile is now read-only
        FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ);
        
        FeatherFileHeader header = FeatherFileHeader.readFrom(channel);
        return createSegmentFile(channel, header.getFileType());
    }

    /**
     * Creates a writer for a new segment file.
     * 
     * @param name The name of the file to create
     * @param type The type of file to create
     * @return A SegmentFileWriter instance for writing to the file
     * @throws IOException If an I/O error occurs
     */
    public SegmentFileWriter createFileWriter(String name, FileType type) throws IOException {
        ensureOpen();
        Path filePath = rootPath.resolve(name + type.getExtension());
        
        return createSegmentFileWriter(filePath, type);
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

    /**
     * Lists all files of a specific type in the storage directory.
     * 
     * @param type The file type to filter by
     * @return Array of file names matching the specified type
     * @throws IOException If an I/O error occurs
     */
    public String[] listFiles(FileType type) throws IOException {
        ensureOpen();
        String extension = type.getExtension();
        try (Stream<Path> paths = Files.list(rootPath)) {
            return paths
                    .filter(p -> p.toString().endsWith(extension))
                    .map(p -> p.getFileName().toString())
                    .toArray(String[]::new);
        }
    }

    @Override
    protected void closeInternal() throws IOException {
        // Reserved for future resource cleanup (caches, buffer pools, etc.)
    }

    /**
     * Creates the appropriate SegmentFile instance based on file type.
     */
    private SegmentFile createSegmentFile(FileChannel channel, FileType type) throws IOException {
        return switch (type) {
            case DOC -> new DocumentFile(channel, BUFFER_SIZE);
            case DIC -> new DictionaryFile(channel, BUFFER_SIZE);
            case POST -> new PostingFile(channel, BUFFER_SIZE);
            case META -> new MetaFile(channel, BUFFER_SIZE);
        };
    }

    /**
     * Creates the appropriate SegmentFileWriter instance based on file type.
     */
    private SegmentFileWriter createSegmentFileWriter(Path path, FileType type) throws IOException {
        return switch (type) {
            case DOC -> new DocumentFileWriter(path, BUFFER_SIZE);
            case DIC -> new DictionaryFileWriter(path, BUFFER_SIZE);
            case POST -> new PostingFileWriter(path, BUFFER_SIZE);
            case META -> {
                // MetaFileWriter requires a SegmentMetadata instance
                // This is a placeholder - actual usage would require passing metadata
                throw new IllegalArgumentException(
                        "MetaFileWriter requires SegmentMetadata. Use createMetaFileWriter() instead.");
            }
        };
    }

    /**
     * Creates a MetaFileWriter with the provided metadata.
     */
    public MetaFileWriter createMetaFileWriter(String name, SegmentMetadata metadata) throws IOException {
        ensureOpen();
        Path filePath = rootPath.resolve(name + FileType.META.getExtension());
        return new MetaFileWriter(filePath, BUFFER_SIZE, metadata);
    }

    /**
     * This method is maintained for backward compatibility but now throws UnsupportedOperationException.
     * Use createFileWriter() instead.
     */
    @Deprecated
    @Override
    public SegmentFile createFile(String name, FileType type) throws IOException {
        throw new UnsupportedOperationException(
                "Direct file creation is no longer supported. Use createSegmentFileWriter() instead.");
    }
}
