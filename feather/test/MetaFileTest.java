import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import storage.exception.InvalidHeaderException;
import storage.file.*;
import storage.writer.MetaFileWriter;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

public class MetaFileTest {
    @TempDir
    Path tempDir;

    private Path filePath;
    private SegmentMetadata metadata;
    private MetaFile file;
    private MetaFileWriter writer;

    @BeforeEach
    void setUp() throws IOException {
        filePath = tempDir.resolve("test.meta");
        metadata = new SegmentMetadata(10, 1, 10);
    }

    @AfterEach
    void tearDown() throws IOException {
        file = null;
        writer = null;
    }

    @Test
    void createNewMetaFile() throws IOException {
        // Given
        writer = new MetaFileWriter(filePath, 8192, metadata);
        
        // When
        file = writer.complete();
        
        // Then
        assertEquals(10, file.getDocumentCount());
        assertEquals(1, file.getMinDocId());
        assertEquals(10, file.getMaxDocId());
    }

    @Test
    void readExistingMetaFile() throws IOException {
        // Given
        writer = new MetaFileWriter(filePath, 8192, metadata);
        file = writer.complete();
        file.close();

        // When
        try (FileChannel readChannel = FileChannel.open(filePath, StandardOpenOption.READ);
             MetaFile reader = new MetaFile(readChannel, 8192)) {
            
            // Then
            assertEquals(10, reader.getDocumentCount());
            assertEquals(1, reader.getMinDocId());
            assertEquals(10, reader.getMaxDocId());
        }
    }

    @Test
    void validateMetadataValues() {
        // Given
        SegmentMetadata invalidMetadata = new SegmentMetadata(5, 10, 1);  // minDocId > maxDocId

        // Then
        assertThrows(IllegalArgumentException.class, () -> {
            new MetaFileWriter(filePath, 8192, invalidMetadata);
        });
    }

    @Test
    void validateDocumentCountRange() {
        // Given
        SegmentMetadata invalidMetadata = new SegmentMetadata(20, 1, 10);  // docCount > range

        // Then
        assertThrows(IllegalArgumentException.class, () -> {
            new MetaFileWriter(filePath, 8192, invalidMetadata);
        });
    }

    @Test
    void validateNegativeDocumentCount() {
        // Given
        SegmentMetadata invalidMetadata = new SegmentMetadata(-1, 1, 10);  // negative docCount

        // Then
        assertThrows(IllegalArgumentException.class, () -> {
            new MetaFileWriter(filePath, 8192, invalidMetadata);
        });
    }

    @Test
    void validateNegativeDocumentId() {
        // Given
        SegmentMetadata invalidMetadata = new SegmentMetadata(5, -1, 10);  // negative minDocId

        // Then
        assertThrows(IllegalArgumentException.class, () -> {
            new MetaFileWriter(filePath, 8192, invalidMetadata);
        });
    }

    @Test
    void verifyFileTypeOnRead() throws IOException {
        // Given - Create a file with wrong type
        Path wrongFilePath = tempDir.resolve("wrong.doc");
        
        // Create a document file instead of meta file
        try (FileChannel channel = FileChannel.open(wrongFilePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE)) {
            
            FeatherFileHeader header = new FeatherFileHeader(FileType.DOC, 0);
            header.writeTo(channel);
        }

        // When & Then
        try (FileChannel channel = FileChannel.open(wrongFilePath, StandardOpenOption.READ)) {
            assertThrows(InvalidHeaderException.class, () -> {
                new MetaFile(channel, 8192);
            });
        }
    }

    @Test
    void verifyCreationTimeIsPreserved() throws IOException {
        // Given
        long beforeCreation = System.currentTimeMillis();
        writer = new MetaFileWriter(filePath, 8192, metadata);
        file = writer.complete();
        
        // When
        long creationTime = file.getCreationTime();
        
        // Then
        assertTrue(creationTime >= beforeCreation);
        assertTrue(creationTime <= System.currentTimeMillis());
    }

    @Test
    void verifyToString() throws IOException {
        // Given
        writer = new MetaFileWriter(filePath, 8192, metadata);
        file = writer.complete();
        
        // When
        String metaString = file.toString();
        
        // Then
        assertTrue(metaString.contains("documents=10"));
        assertTrue(metaString.contains("docIds=[1,10]"));
    }
}
