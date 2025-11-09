import core.analysis.FeatherAnalyzer;
import core.analysis.LuceneAnalyzerAdapter;
import core.index.IndexWriter;
import core.index.IndexWriterConfig;
import core.index.Segments;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import storage.FileSystemStorage;
import storage.Storage;
import storage.file.Document;
import storage.file.FileType;
import storage.file.SegmentMetadata;
import storage.merge.MergePolicy;
import storage.merge.MergeSpec;
import storage.writer.SegmentFileWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class IndexWriterTest {

    @TempDir
    Path tempDir;

    private Storage storage;
    private FeatherAnalyzer analyzer;
    private IndexWriterConfig config;
    private IndexWriter writer;
    private MergePolicy mergePolicy;

    @BeforeEach
    void setUp() throws IOException {
        storage = new FileSystemStorage(tempDir);
        analyzer = new LuceneAnalyzerAdapter(new StandardAnalyzer());
        mergePolicy = new MergePolicy() {
            // This anonymous MergePolicy stub is used only for unit testing
            @Override
            public MergeSpec findMerges(List<SegmentMetadata> segments) {
                return null;
            }
        };
        config = new IndexWriterConfig(analyzer, mergePolicy, 10); // maxBufferedDocs = 10
        writer = new IndexWriter(storage, config);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }

    private Document createTestDocument(int id, String content) {
        Document doc = new Document(id);
        doc.addField("content", content);
        return doc;
    }

    @Test
    void shouldCreateSegmentFilesAndNotCommitFiles_WhenFlushIsSuccessful() throws IOException {
        // Given
        writer.addDocument(createTestDocument(1, "First document"));
        writer.addDocument(createTestDocument(2, "Second document"));

        // When
        writer.flush();

        // Then
        // Check that segment files were created
        assertTrue(Files.exists(tempDir.resolve("segment_0" + FileType.DOC.getExtension())), "segment_0.doc should exist");
        assertTrue(Files.exists(tempDir.resolve("segment_0" + FileType.POST.getExtension())), "segment_0.post should exist");
        assertTrue(Files.exists(tempDir.resolve("segment_0" + FileType.DIC.getExtension())), "segment_0.dic should exist");
        assertTrue(Files.exists(tempDir.resolve("segment_0" + FileType.META.getExtension())), "segment_0.meta should exist");

        // Check that commit point files were NOT created yet, as commit() was not called
        assertFalse(Files.exists(tempDir.resolve("segments_0")), "segments_0 should not exist before commit");
        assertFalse(Files.exists(tempDir.resolve(Segments.SEGMENTS_GEN)), "segments.gen should not exist before commit");
    }

    @Test
    void shouldCreateCommitPointFiles_WhenCommitIsSuccessful() throws IOException {
        // Given
        writer.addDocument(createTestDocument(1, "A document to commit"));

        // When
        writer.commit();

        // Then
        // Check that segment files were created
        assertTrue(Files.exists(tempDir.resolve("segment_0" + FileType.DOC.getExtension())), "segment_0.doc should exist");

        // Check that commit point files were created
        assertTrue(Files.exists(tempDir.resolve("segments_0")), "segments_0 should exist after commit");
        assertTrue(Files.exists(tempDir.resolve(Segments.SEGMENTS_GEN)), "segments.gen should exist after commit");

        // Verify the content of segments.gen
        String genContent = Files.readString(tempDir.resolve(Segments.SEGMENTS_GEN));
        assertEquals("0", genContent.trim(), "segments.gen should contain '0'");
    }

    @Test
    void shouldCleanupPartialSegmentFiles_WhenFlushFails() {
        // Given
        Storage faultyStorage = new FaultyStorage(tempDir);
        Exception thrownException = null;

        try (IndexWriter faultyWriter = new IndexWriter(faultyStorage, config)) {
            faultyWriter.addDocument(createTestDocument(1, "This flush should fail"));
            faultyWriter.flush(); // This call is expected to throw an IOException
        } catch (IOException e) {
            thrownException = e;
        }

        // Then
        assertNotNull(thrownException, "An IOException should have been thrown during flush.");
        assertTrue(thrownException.getMessage().contains("Failed to flush segment segment_0"), "Exception message should indicate flush failure.");

        // Assert that all potential segment files were cleaned up
        assertFalse(Files.exists(tempDir.resolve("segment_0" + FileType.DOC.getExtension())), "Partial .doc file should be cleaned up.");
        assertFalse(Files.exists(tempDir.resolve("segment_0" + FileType.POST.getExtension())), "Partial .post file should be cleaned up.");
        assertFalse(Files.exists(tempDir.resolve("segment_0" + FileType.DIC.getExtension())), "Partial .dic file should be cleaned up.");
        assertFalse(Files.exists(tempDir.resolve("segment_0" + FileType.META.getExtension())), "Partial .meta file should be cleaned up.");
    }

    @Test
    void shouldInitializeSegmentCounterCorrectly_WhenOpeningExistingIndex() throws IOException {
        // Given: An existing index with one segment committed
        writer.addDocument(createTestDocument(1, "doc1 for first writer"));
        writer.commit();
        writer.close(); // Close the first writer to simulate a restart, this also closes the storage

        // When: A new writer is created on the same directory
        Storage newStorage = new FileSystemStorage(tempDir);
        IndexWriter newWriter = new IndexWriter(newStorage, config);

        // Add a new document and flush to verify a new segment is created with the correct name
        newWriter.addDocument(createTestDocument(2, "doc2 for second writer"));
        newWriter.flush();

        // Assert that the new segment is segment_1, which reflects the segmentCounter field
        assertTrue(Files.exists(tempDir.resolve("segment_1" + FileType.DOC.getExtension())), "segment_1.doc should exist");
        newWriter.close();
    }

    @Test
    void shouldAutomaticallyFlushDocuments_WhenMaxBufferedDocsIsReached() throws IOException {
        // Given: A config with maxBufferedDocs = 2
        config = new IndexWriterConfig(analyzer, mergePolicy, 2);
        writer.close(); // Close the old writer to apply new config, this also closes the storage
        Storage newStorage = new FileSystemStorage(tempDir);
        writer = new IndexWriter(newStorage, config);

        // When: We add exactly maxBufferedDocs documents
        writer.addDocument(createTestDocument(1, "doc1"));
        writer.addDocument(createTestDocument(2, "doc2")); // This second addDocument() call should trigger flush()

        // Then: Segment files for segment_0 should exist without an explicit flush() call
        assertTrue(Files.exists(tempDir.resolve("segment_0" + FileType.DOC.getExtension())), "segment_0.doc should exist due to auto-flush");
        assertTrue(Files.exists(tempDir.resolve("segment_0" + FileType.POST.getExtension())), "segment_0.post should exist due to auto-flush");
        assertTrue(Files.exists(tempDir.resolve("segment_0" + FileType.DIC.getExtension())), "segment_0.dic should exist due to auto-flush");
        assertTrue(Files.exists(tempDir.resolve("segment_0" + FileType.META.getExtension())), "segment_0.meta should exist due to auto-flush");
    }

    @Test
    void shouldNotFlush_WhenDocumentBufferIsEmpty() throws IOException {
        // Given: An empty document buffer
        // When: flush() is called
        writer.flush();

        // Then: No segment files should be created
        assertFalse(Files.exists(tempDir.resolve("segment_0" + FileType.DOC.getExtension())), "No segment files should be created for empty buffer");
        assertFalse(Files.exists(tempDir.resolve(Segments.SEGMENTS_GEN)), "No commit files should be created for empty buffer");
    }

    @Test
    void shouldThrowNullPointerException_WhenAnalyzerIsNullInConfig() throws IOException {
        // Given: A null analyzer is to be used
        writer.close(); // Close current writer to release resources

        // When & Then: Creating an IndexWriterConfig with a null analyzer should throw NullPointerException
        assertThrows(NullPointerException.class, () -> {
            new IndexWriterConfig(null, mergePolicy, 10);
        }, "IndexWriterConfig constructor should throw NullPointerException if analyzer is null.");
    }


    /**
     * A custom Storage implementation that throws an exception during the creation
     * of the second file writer (.post) to simulate a failure during flush.
     */
    private static class FaultyStorage extends FileSystemStorage {
        private boolean firstDocWriterCreated = false;

        public FaultyStorage(Path rootPath) {
            super(rootPath);
        }

        @Override
        public SegmentFileWriter createFileWriter(String name, FileType type) throws IOException {
            // Allow the .doc file to be created successfully
            if (type == FileType.DOC && !firstDocWriterCreated) {
                firstDocWriterCreated = true;
                return super.createFileWriter(name, type);
            }
            // Fail on the next file type (.post)
            if (type == FileType.POST) {
                throw new IOException("Simulated I/O error on creating .post file");
            }
            return super.createFileWriter(name, type);
        }
    }
}
