import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import storage.file.Document;
import storage.file.DocumentFile;
import storage.file.FeatherFileHeader;
import storage.file.FileType;
import storage.writer.DocumentFileWriter;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

class DocumentFileTest {
    @TempDir
    Path tempDir;

    private Path filePath;
    private FileChannel channel;
    private DocumentFile file;
    private DocumentFileWriter writer;

    @BeforeEach
    void setUp() throws IOException {
        filePath = tempDir.resolve("test.bin");
        FeatherFileHeader header = new FeatherFileHeader(FileType.DOC, 0);
        writer = new DocumentFileWriter(filePath, 8192);
    }

    @AfterEach
    void tearDown() throws IOException {
        // writable channels always get closed when complete()
        writer = null;
        file = null;
    }

    @Test
    void writeAndReadDocument() throws IOException, InterruptedException {
        // Given
        Document doc = new Document(1);
        doc.addField("title", "Test Document");
        doc.addField("content", "This is a test content");
        doc.addField("views", 42);
        doc.addField("data", new byte[]{1, 2, 3, 4});

        // When
        writer.writeDocument(doc);
        file = writer.complete();

        file.seekToContent();
        Document readDoc = file.readDocument();

        // Then
        assertEquals(1, readDoc.getId());
        assertEquals("Test Document", readDoc.getField("title"));
        assertEquals("This is a test content", readDoc.getField("content"));
        assertEquals(42L, readDoc.getField("views"));
        assertArrayEquals(new byte[]{1, 2, 3, 4}, (byte[]) readDoc.getField("data"));
    }

    @Test
    void writeAndReadMultipleDocuments() throws IOException {
        // Given
        Document doc1 = new Document(1);
        doc1.addField("title", "First Document");

        Document doc2 = new Document(2);
        doc2.addField("title", "Second Document");

        // When
        writer.writeDocument(doc1);
        writer.writeDocument(doc2);
        file = writer.complete();

        file.seekToContent();
        Document readDoc1 = file.readDocument();
        Document readDoc2 = file.readDocument();

        // Then
        assertEquals("First Document", readDoc1.getField("title"));
        assertEquals("Second Document", readDoc2.getField("title"));
    }

    @Test
    void writeAndReadLargeDocument() throws IOException {
        // Given
        Document doc = new Document(1);
        StringBuilder largeContent = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            largeContent.append("Large content line ").append(i).append("\n");
        }
        doc.addField("content", largeContent.toString());

        // When
        writer.writeDocument(doc);
        file = writer.complete();

        file.seekToContent();
        System.out.println(file.size());
        Document readDoc = file.readDocument();

        // Then
        assertEquals(largeContent.toString(), readDoc.getField("content"));
    }

    @Test
    void writeAndReadAllFieldTypes() throws IOException {
        // Given
        Document doc = new Document(1);
        doc.addField("string", "text value");
        doc.addField("number", 123456789L);
        doc.addField("binary", new byte[]{5, 6, 7, 8});

        // When
        writer.writeDocument(doc);
        file = writer.complete();

        file.seekToContent();
        Document readDoc = file.readDocument();

        // Then
        assertEquals("text value", readDoc.getField("string"));
        assertEquals(123456789L, readDoc.getField("number"));
        assertArrayEquals(new byte[]{5, 6, 7, 8}, (byte[]) readDoc.getField("binary"));
    }

    @Test
    void verifyFileSize() throws IOException {
        // Given
        Document doc = new Document(1);
        doc.addField("title", "Test");

        // When
        writer.writeDocument(doc);
        file = writer.complete();

        // Then
        long expectedSize = FeatherFileHeader.HEADER_SIZE +
                4 +
                4 +
                4 +
                2 + "title".getBytes(StandardCharsets.UTF_8).length +
                1 +
                4 + "Test".getBytes(StandardCharsets.UTF_8).length;

        assertEquals(expectedSize, file.size());
    }

    @Test
    void verifyDocumentsAfterFlush() throws IOException {
        // Given
        Document doc1 = new Document(1);
        doc1.addField("title", "First Document");
        Document doc2 = new Document(2);
        doc2.addField("title", "Second Document");

        // When
        writer.writeDocument(doc1);
        writer.writeDocument(doc2);
        file = writer.complete();

        try (FileChannel newChannel = FileChannel.open(filePath, StandardOpenOption.READ);
             DocumentFile newDocFile = new DocumentFile(newChannel, 8192)) {

            newDocFile.seekToContent();
            Document readDoc1 = newDocFile.readDocument();
            Document readDoc2 = newDocFile.readDocument();

            assertEquals("First Document", readDoc1.getField("title"));
            assertEquals("Second Document", readDoc2.getField("title"));
        }
    }
}
