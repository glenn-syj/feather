import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import storage.exception.InvalidHeaderException;
import storage.file.*;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MetaFileTest {
    @TempDir
    Path tempDir;

    private Path filePath;
    private FileChannel channel;
    private FeatherFileHeader header;
    private SegmentMetadata metadata;
    private MetaFile metaFile;

    @BeforeEach
    void setUp() throws IOException {
        filePath = tempDir.resolve("test.meta");
        channel = FileChannel.open(filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);

        header = new FeatherFileHeader(FileType.META, 10);
        metadata = new SegmentMetadata(10, 1, 10);
        metaFile = new MetaFile(channel, 8192, header, metadata);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (metaFile != null) {
            metaFile.close();
        }
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }

    @Test
    void createNewMetaFile() throws IOException {
        // Given, When - @BeforeEach
        // Then
        assertEquals(10, metaFile.getDocumentCount());
        assertEquals(1, metaFile.getMinDocId());
        assertEquals(10, metaFile.getMaxDocId());
    }

    @Test
    void readExistingMetaFile() throws IOException {
        // Given
        metaFile.close();

        // When
        FileChannel readChannel = FileChannel.open(filePath,
                                    StandardOpenOption.READ,
                                    StandardOpenOption.WRITE,
                                    StandardOpenOption.CREATE);
        MetaFile reader = new MetaFile(readChannel, 8192, header, metadata);

        // Then
        assertEquals(10, reader.getDocumentCount());
        assertEquals(1, reader.getMinDocId());
        assertEquals(10, reader.getMaxDocId());
    }

    @Test
    void validateHeaderAndMetadataConsistency() {
        // Given
        FeatherFileHeader invalidHeader = new FeatherFileHeader(FileType.META, 5);
        SegmentMetadata validMetadata = new SegmentMetadata(10, 1, 10);

        // Then
        assertThrows(IllegalArgumentException.class, () -> {
            new MetaFile(channel, 8192, invalidHeader, validMetadata);
        });
    }

    @Test
    void validateMetadataValues() {
        // Given & When
        FeatherFileHeader validHeader = new FeatherFileHeader(FileType.META, 5);
        SegmentMetadata invalidMetadata = new SegmentMetadata(5, 10, 1);  // minDocId > maxDocId

        // Then
        assertThrows(IllegalArgumentException.class, () -> {
            new MetaFile(channel, 8192, validHeader, invalidMetadata);
        });
    }

    @Test
    void checkFileType() throws IOException {
        // Given & When
        FeatherFileHeader wrongHeader = new FeatherFileHeader(FileType.DOC, 10);

        // Then
        assertThrows(InvalidHeaderException.class, () -> {
            new MetaFile(channel, 8192, wrongHeader, metadata);
        });
    }
}
