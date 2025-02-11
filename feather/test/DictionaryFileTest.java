import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import storage.file.DictionaryFile;
import storage.file.FeatherFileHeader;
import storage.file.FileType;
import storage.file.Term;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

public class DictionaryFileTest {
    private static final int BUFFER_SIZE = 8192;
    private static final int INDEX_BLOCK_SIZE = 128;

    @TempDir
    Path tempDir;

    private DictionaryFile dictionaryFile;
    private Path filePath;
    private FileChannel channel;

    @BeforeEach
    void setUp() throws IOException {
        filePath = tempDir.resolve("test.dic");
        Files.deleteIfExists(filePath);

        channel = FileChannel.open(filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);

        FeatherFileHeader header = new FeatherFileHeader(FileType.DIC, 0);
        header.writeTo(channel);

        dictionaryFile = new DictionaryFile(channel, BUFFER_SIZE, header);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }

    @Test
    void writeAndFindSingleTerm() throws IOException {
        // Given
        Term term = new Term("title", "apple", 1, 1000L);

        // When
        dictionaryFile.addTermRecord(term);
        dictionaryFile.flush();

        // Then
        Term found = dictionaryFile.findTerm("title", "apple");
        assertNotNull(found);
        assertEquals("title", found.getField());
        assertEquals("apple", found.getText());
        assertEquals(1, found.getDocumentFrequency());
        assertEquals(1000L, found.getPostingPosition());
    }

    @Test
    void writeAndFindMultipleTermsInSingleBlock() throws IOException {
        // Given - Terms that can fit in a single block
        Term[] terms = {
                new Term("title", "apple", 1, 1000L),
                new Term("title", "banana", 2, 2000L),
                new Term("title", "cherry", 3, 3000L),
                new Term("content", "apple", 4, 4000L)
        };

        // When
        for (Term term : terms) {
            dictionaryFile.addTermRecord(term);
        }
        dictionaryFile.flush();

        // Then
        Term found1 = dictionaryFile.findTerm("title", "banana");
        assertNotNull(found1);
        assertEquals("banana", found1.getText());
        assertEquals(2, found1.getDocumentFrequency());

        Term found2 = dictionaryFile.findTerm("content", "apple");
        assertNotNull(found2);
        assertEquals("content", found2.getField());
        assertEquals(4, found2.getDocumentFrequency());
    }

    @Test
    void writeAndFindTermsAcrossMultipleBlocks() throws IOException {
        // Given - Create INDEX_BLOCK_SIZE + 1 terms
        Term[] terms = new Term[INDEX_BLOCK_SIZE + 1];
        for (int i = 0; i < terms.length; i++) {
            terms[i] = new Term("title", "term" + i, i, i * 1000L);
        }

        // When
        for (Term term : terms) {
            dictionaryFile.addTermRecord(term);
        }
        dictionaryFile.flush();

        // Then
        // Term in first block
        Term found1 = dictionaryFile.findTerm("title", "term0");
        assertNotNull(found1);
        assertEquals(0L, found1.getPostingPosition());

        // Term in second block
        Term found2 = dictionaryFile.findTerm("title", "term" + INDEX_BLOCK_SIZE);
        assertNotNull(found2);
        assertEquals(INDEX_BLOCK_SIZE * 1000L, found2.getPostingPosition());
    }

    @Test
    void findNonExistentTerm() throws IOException {
        // Given
        Term term = new Term("title", "apple", 1, 1000L);
        dictionaryFile.addTermRecord(term);
        dictionaryFile.flush();

        // When & Then
        assertNull(dictionaryFile.findTerm("title", "banana"));
        assertNull(dictionaryFile.findTerm("content", "apple"));
    }

    @Test
    void testLongTermPrefix() throws IOException {
        // Given
        String longText = "verylongwordthatexceedsprefixlength";
        Term term = new Term("title", longText, 1, 1000L);

        // When
        dictionaryFile.addTermRecord(term);
        dictionaryFile.flush();

        // Then
        Term found = dictionaryFile.findTerm("title", longText);
        assertNotNull(found);
        assertEquals(longText, found.getText());
    }

    @Test
    void testBlockBoundarySearch() throws IOException {
        // Given - Terms spanning across block boundaries
        Term[] terms = new Term[INDEX_BLOCK_SIZE * 2];  // Spans two blocks
        for (int i = 0; i < terms.length; i++) {
            terms[i] = new Term("title", String.format("term%03d", i), i, i * 1000L);
        }

        // When
        for (Term term : terms) {
            dictionaryFile.addTermRecord(term);
        }
        dictionaryFile.flush();

        // Then
        // Search terms near block boundaries
        Term found1 = dictionaryFile.findTerm("title",
                String.format("term%03d", INDEX_BLOCK_SIZE - 1));  // Last term in first block
        Term found2 = dictionaryFile.findTerm("title",
                String.format("term%03d", INDEX_BLOCK_SIZE));      // First term in second block

        assertNotNull(found1);
        assertNotNull(found2);
        assertEquals((INDEX_BLOCK_SIZE - 1) * 1000L, found1.getPostingPosition());
        assertEquals(INDEX_BLOCK_SIZE * 1000L, found2.getPostingPosition());
    }

    @Test
    void testUnicodeTerms() throws IOException {
        // Given
        Term[] terms = {
                new Term("title", "\uC0AC\uACFC", 1, 1000L),
                new Term("title", "\uBC14\uB098\uB098", 2, 2000L),
                new Term("content", "\uD55C\uAE00", 3, 3000L)
        };

        // When
        for (Term term : terms) {
            dictionaryFile.addTermRecord(term);
        }
        dictionaryFile.flush();

        // Then
        Term found = dictionaryFile.findTerm("title", "\uBC14\uB098\uB098");
        assertNotNull(found);
        assertEquals("\uBC14\uB098\uB098", found.getText());
        assertEquals(2000L, found.getPostingPosition());
    }
}
