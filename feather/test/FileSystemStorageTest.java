import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import storage.FileSystemStorage;
import storage.file.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;


/* TODO: Design issues found during testing:
 * 1. Current design allows file modification after creation
 *    - shouldHandleFileHeaderCorrectly test shows we can open and modify files
 *    - This violates Lucene-style immutable segment file model
 *    - Can lead to concurrency issues when multiple threads access the same file
 *    - May cause consistency problems between memory and disk states
 *    - Cache coherency issues when multiple instances read the same file
 *
 * 2. No separation between file creation and reading
 *    - createFile() returns modifiable SegmentFile instance
 *    - Should use separate Writer for creation and SegmentFile for reading
 *
 * 3. Missing validation for file state
 *    - No guarantee that file is properly initialized before use
 *    - Need to ensure all required data is written during creation
 */
class FileSystemStorageTest {
    @TempDir
    Path tempDir;

    private FileSystemStorage storage;

    @BeforeEach
    void setUp() {
        storage = new FileSystemStorage(tempDir);
    }

    @Test
    void shouldThrowExceptionWhenClosed() throws IOException {
        storage.close();

        assertThrows(IllegalStateException.class, () -> storage.createFile("test.doc", FileType.DOC));
        assertThrows(IllegalStateException.class, () -> storage.openFile("test.doc"));
        assertThrows(IllegalStateException.class, () -> storage.deleteFile("test.doc"));
        assertThrows(IllegalStateException.class, () -> storage.listFiles());
    }

    @Test
    void shouldCreateAndOpenFile() throws IOException {
        // Given
        String fileName = "test.doc";

        // When
        SegmentFile created = storage.createFile(fileName, FileType.DOC);

        // Then
        assertNotNull(created);
        assertTrue(created instanceof DocumentFile);
        assertTrue(Files.exists(tempDir.resolve(fileName)));

        // When
        SegmentFile opened = storage.openFile(fileName);

        // Then
        assertNotNull(opened);
        assertTrue(opened instanceof DocumentFile);
    }

    @Test
    void shouldHandleDifferentFileTypes() throws IOException {
        // Test each file type
        testFileType("test.doc", FileType.DOC, DocumentFile.class);
        testFileType("test.dic", FileType.DIC, DictionaryFile.class);
        testFileType("test.post", FileType.POST, PostingFile.class);
        testFileType("test.meta", FileType.META, MetaFile.class);
    }

    private void testFileType(String fileName, FileType type, Class<?> expectedClass) throws IOException {
        SegmentFile file = storage.createFile(fileName, type);
        assertNotNull(file);
        assertTrue(expectedClass.isInstance(file));
        assertTrue(Files.exists(tempDir.resolve(fileName)));
    }

    @Test
    void shouldListAllFiles() throws IOException {
        // Given
        storage.createFile("1.doc", FileType.DOC);
        storage.createFile("2.dic", FileType.DIC);
        storage.createFile("3.post", FileType.POST);

        // When
        String[] files = storage.listFiles();

        // Then
        assertNotNull(files);
        assertEquals(3, files.length);
        assertTrue(containsFile(files, "1.doc"));
        assertTrue(containsFile(files, "2.dic"));
        assertTrue(containsFile(files, "3.post"));
    }

    private boolean containsFile(String[] files, String fileName) {
        for (String file : files) {
            if (file.equals(fileName)) return true;
        }
        return false;
    }

    @Test
    void shouldListEmptyDirectoryWhenNew() throws IOException {
        String[] files = storage.listFiles();
        assertNotNull(files);
        assertEquals(0, files.length);
    }

    @Test
    void shouldDeleteFile() throws IOException {
        // Given
        String fileName = "test.doc";
        storage.createFile(fileName, FileType.DOC);
        assertTrue(Files.exists(tempDir.resolve(fileName)));

        // When
        storage.deleteFile(fileName);

        // Then
        assertFalse(Files.exists(tempDir.resolve(fileName)));
    }

    @Test
    void shouldDeleteNonExistentFileWithoutError() throws IOException {
        assertDoesNotThrow(() -> storage.deleteFile("nonexistent.doc"));
    }

    @Test
    void shouldThrowWhenOpeningNonExistentFile() {
        assertThrows(IOException.class, () -> storage.openFile("nonexistent.doc"));
    }

    @Test
    void shouldHandleFileHeaderCorrectly() throws IOException {
        // Given
        String fileName = "test.doc";

        // When
        SegmentFile created = storage.createFile(fileName, FileType.DOC);
        created.close();

        SegmentFile opened = storage.openFile(fileName);

        // Then
        assertEquals(FileType.DOC, opened.getHeaderFileType());
    }

    @Test
    void shouldCreateDirectoryIfNotExists() {
        Path newDir = tempDir.resolve("subdir");
        FileSystemStorage newStorage = new FileSystemStorage(newDir);
        assertTrue(Files.exists(newDir));
    }

    @Test
    void shouldThrowWhenCreatingInvalidDirectory() {
        Path invalidPath = tempDir.resolve("invalid\0path");
        assertThrows(InvalidPathException.class, () -> new FileSystemStorage(invalidPath));
    }
}