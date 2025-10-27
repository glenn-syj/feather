import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import storage.FileSystemStorage;
import storage.file.*;
import storage.writer.MetaFileWriter;
import storage.writer.SegmentFileWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

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
        assertThrows(IllegalStateException.class, () -> storage.createFileWriter("test", FileType.DOC));
        assertThrows(IllegalStateException.class, () -> storage.openFile("test.doc"));
        assertThrows(IllegalStateException.class, () -> storage.deleteFile("test.doc"));
        assertThrows(IllegalStateException.class, () -> storage.listFiles());
    }

    @Test
    void shouldCreateAndOpenFile() throws IOException {
        // Given
        String fileName = "test";

        // When
        SegmentFileWriter writer = storage.createFileWriter(fileName, FileType.DOC);
        writer.complete();

        // Then
        assertTrue(Files.exists(tempDir.resolve(fileName + FileType.DOC.getExtension())));

        // When
        SegmentFile opened = storage.openFile(fileName + FileType.DOC.getExtension());

        // Then
        assertNotNull(opened);
        assertTrue(opened instanceof DocumentFile);
    }

    @Test
    void shouldHandleDifferentFileTypes() throws IOException {
        testFileType("test", FileType.DOC, DocumentFile.class);
        testFileType("test", FileType.DIC, DictionaryFile.class);
        testFileType("test", FileType.POST, PostingFile.class);
        
        // MetaFile requires metadata
        SegmentMetadata metadata = new SegmentMetadata(0, 0, 0);
        MetaFileWriter writer = storage.createMetaFileWriter("test", metadata);
        SegmentFile file = writer.complete();
        assertTrue(file instanceof MetaFile);
    }

    private void testFileType(String fileName, FileType type, Class<?> expectedClass) throws IOException {
        SegmentFileWriter writer = storage.createFileWriter(fileName, type);
        SegmentFile file = writer.complete();
        assertNotNull(file);
        assertTrue(expectedClass.isInstance(file));
        assertTrue(Files.exists(tempDir.resolve(fileName + type.getExtension())));
    }

    @Test
    void shouldListAllFiles() throws IOException {
        // Given
        createTestFile("1", FileType.DOC);
        createTestFile("2", FileType.DIC);
        createTestFile("3", FileType.POST);

        // When
        String[] files = storage.listFiles();

        // Then
        assertNotNull(files);
        assertEquals(3, files.length);
        assertTrue(containsFile(files, "1.doc"));
        assertTrue(containsFile(files, "2.dic"));
        assertTrue(containsFile(files, "3.post"));
    }

    @Test
    void shouldListFilesByType() throws IOException {
        // Given
        createTestFile("1", FileType.DOC);
        createTestFile("2", FileType.DOC);
        createTestFile("3", FileType.DIC);

        // When
        String[] docFiles = storage.listFiles(FileType.DOC);
        String[] dicFiles = storage.listFiles(FileType.DIC);

        // Then
        assertEquals(2, docFiles.length);
        assertEquals(1, dicFiles.length);
        assertTrue(containsFile(docFiles, "1.doc"));
        assertTrue(containsFile(docFiles, "2.doc"));
        assertTrue(containsFile(dicFiles, "3.dic"));
    }

    private void createTestFile(String name, FileType type) throws IOException {
        SegmentFileWriter writer = storage.createFileWriter(name, type);
        writer.complete();
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
        String fileName = "test";
        SegmentFileWriter writer = storage.createFileWriter(fileName, FileType.DOC);
        writer.complete();
        String fullName = fileName + FileType.DOC.getExtension();
        assertTrue(Files.exists(tempDir.resolve(fullName)));

        // When
        storage.deleteFile(fullName);

        // Then
        assertFalse(Files.exists(tempDir.resolve(fullName)));
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
        String fileName = "test";

        // When
        SegmentFileWriter writer = storage.createFileWriter(fileName, FileType.DOC);
        SegmentFile file = writer.complete();

        // Then
        assertEquals(FileType.DOC, file.getHeaderFileType());
        file.close();
    }

    @Test
    void shouldCreateDirectoryIfNotExists() {
        Path newDir = tempDir.resolve("subdir");
        FileSystemStorage newStorage = new FileSystemStorage(newDir);
        assertTrue(Files.exists(newDir));
    }

    @Test
    void shouldThrowWhenCreatingInvalidDirectory() {
        assertThrows(InvalidPathException.class,
                () -> new FileSystemStorage(tempDir.resolve("invalid<path")));
        assertThrows(InvalidPathException.class,
                () -> new FileSystemStorage(tempDir.resolve("invalid>path")));
        assertThrows(InvalidPathException.class,
                () -> new FileSystemStorage(tempDir.resolve("invalid:path")));
        assertThrows(InvalidPathException.class,
                () -> new FileSystemStorage(tempDir.resolve("invalid\"path")));
        assertThrows(InvalidPathException.class,
                () -> new FileSystemStorage(tempDir.resolve("invalid|path")));
        assertThrows(InvalidPathException.class,
                () -> new FileSystemStorage(tempDir.resolve("invalid?path")));
        assertThrows(InvalidPathException.class,
                () -> new FileSystemStorage(tempDir.resolve("invalid*path")));
    }

    @Test
    void shouldThrowWhenCreatingFileWithExistingName() throws IOException {
        // Given
        String fileName = "test";
        SegmentFileWriter writer1 = storage.createFileWriter(fileName, FileType.DOC);
        writer1.complete();

        // When & Then
        assertThrows(IOException.class, () -> storage.createFileWriter(fileName, FileType.DOC));
    }

    @Test
    void shouldThrowWhenUsingDeprecatedCreateFile() throws IOException {
        assertThrows(UnsupportedOperationException.class, 
            () -> storage.createFile("test", FileType.DOC));
    }

    @Test
    void shouldCreateAndOpenMetaFile() throws IOException {
        // Given
        String fileName = "test";
        SegmentMetadata metadata = new SegmentMetadata(10, 1, 10);  // docCount, minDocId, maxDocId

        // When
        MetaFileWriter writer = storage.createMetaFileWriter(fileName, metadata);
        SegmentFile file = writer.complete();

        // Then
        assertTrue(Files.exists(tempDir.resolve(fileName + FileType.META.getExtension())));

        // When
        SegmentFile opened = storage.openFile(fileName + FileType.META.getExtension());

        // Then
        assertNotNull(opened);
        assertTrue(opened instanceof MetaFile);
        assertEquals(FileType.META, opened.getHeaderFileType());
    }

    @Test
    void shouldThrowWhenCreatingMetaFileWithNullMetadata() {
        String fileName = "test";
        assertThrows(IllegalArgumentException.class, 
            () -> storage.createMetaFileWriter(fileName, null));
    }

    @Test
    void shouldThrowWhenCreatingMetaFileWithInvalidMetadata() {
        String fileName = "test";
        
        // Invalid document count
        assertThrows(IllegalArgumentException.class,
            () -> storage.createMetaFileWriter(fileName + "1", new SegmentMetadata(-1, 0, 0)));

        // Invalid document ID range
        assertThrows(IllegalArgumentException.class, 
            () -> storage.createMetaFileWriter(fileName + "2", new SegmentMetadata(1, 10, 5)));
        
        // Negative document ID with non-zero count
        assertThrows(IllegalArgumentException.class, 
            () -> storage.createMetaFileWriter(fileName + "3", new SegmentMetadata(1, -1, 1)));
        
        // Document count exceeds ID range
        assertThrows(IllegalArgumentException.class, 
            () -> storage.createMetaFileWriter(fileName + "4", new SegmentMetadata(3, 1, 2)));
    }

    @Test
    void shouldCheckFileExists() throws IOException {
        // Given
        String fileName = "test";
        String fullName = fileName + FileType.DOC.getExtension();
        
        // When - file doesn't exist
        boolean existsBefore = storage.fileExists(fullName);
        
        // Then
        assertFalse(existsBefore);
        
        // When - create file
        SegmentFileWriter writer = storage.createFileWriter(fileName, FileType.DOC);
        writer.complete();
        
        // When - file exists
        boolean existsAfter = storage.fileExists(fullName);
        
        // Then
        assertTrue(existsAfter);
    }

    @Test
    void shouldThrowWhenCheckingFileExistsWithInvalidName() {
        // Test null name
        assertThrows(IllegalArgumentException.class, () -> storage.fileExists(null));
        
        // Test empty name
        assertThrows(IllegalArgumentException.class, () -> storage.fileExists(""));
        assertThrows(IllegalArgumentException.class, () -> storage.fileExists("   "));
        
        // Test invalid characters
        assertThrows(IllegalArgumentException.class, () -> storage.fileExists("../test.doc"));
        assertThrows(IllegalArgumentException.class, () -> storage.fileExists("test/../doc"));
        assertThrows(IllegalArgumentException.class, () -> storage.fileExists("test\\doc"));
    }

    @Test
    void shouldGetFileLength() throws IOException {
        // Given
        String fileName = "test";
        String fullName = fileName + FileType.DOC.getExtension();
        
        // When - create file
        SegmentFileWriter writer = storage.createFileWriter(fileName, FileType.DOC);
        SegmentFile file = writer.complete();
        file.close();
        
        // When - get file length
        long length = storage.fileLength(fullName);
        
        // Then
        assertTrue(length > 0);
        assertEquals(Files.size(tempDir.resolve(fullName)), length);
    }

    @Test
    void shouldThrowWhenGettingLengthOfNonExistentFile() {
        String fileName = "nonexistent.doc";
        assertThrows(IOException.class, () -> storage.fileLength(fileName));
    }

    @Test
    void shouldThrowWhenGettingFileLengthWithInvalidName() {
        // Test null name
        assertThrows(IllegalArgumentException.class, () -> storage.fileLength(null));
        
        // Test empty name
        assertThrows(IllegalArgumentException.class, () -> storage.fileLength(""));
        assertThrows(IllegalArgumentException.class, () -> storage.fileLength("   "));
        
        // Test invalid characters
        assertThrows(IllegalArgumentException.class, () -> storage.fileLength("../test.doc"));
        assertThrows(IllegalArgumentException.class, () -> storage.fileLength("test/../doc"));
        assertThrows(IllegalArgumentException.class, () -> storage.fileLength("test\\doc"));
    }

    @Test
    void shouldThrowWhenClosedForFileLengthAndExists() throws IOException {
        storage.close();
        assertThrows(IllegalStateException.class, () -> storage.fileLength("test.doc"));
        assertThrows(IllegalStateException.class, () -> storage.fileExists("test.doc"));
    }

    @Test
    void shouldHandleFileLengthForDifferentFileTypes() throws IOException {
        // Test DOC file
        testFileLength("test1", FileType.DOC);
        
        // Test DIC file
        testFileLength("test2", FileType.DIC);
        
        // Test POST file
        testFileLength("test3", FileType.POST);
        
        // Test META file
        SegmentMetadata metadata = new SegmentMetadata(5, 1, 5);
        MetaFileWriter writer = storage.createMetaFileWriter("test4", metadata);
        SegmentFile file = writer.complete();
        file.close();
        
        long metaLength = storage.fileLength("test4.meta");
        assertTrue(metaLength > 0);
    }

    private void testFileLength(String fileName, FileType type) throws IOException {
        String fullName = fileName + type.getExtension();
        
        SegmentFileWriter writer = storage.createFileWriter(fileName, type);
        SegmentFile file = writer.complete();
        file.close();
        
        long length = storage.fileLength(fullName);
        assertTrue(length > 0);
        assertEquals(Files.size(tempDir.resolve(fullName)), length);
    }

    // ========== RENAME TESTS ==========

    @Test
    void shouldRenameFile() throws IOException {
        // Given
        String sourceName = "source.doc";
        String destName = "dest.doc";
        createTestFile("source", FileType.DOC);
        
        // When
        storage.rename(sourceName, destName);
        
        // Then
        assertFalse(storage.fileExists(sourceName));
        assertTrue(storage.fileExists(destName));
        assertEquals(storage.fileLength(destName), Files.size(tempDir.resolve(destName)));
    }

    @Test
    void shouldRenameMultipleFileTypes() throws IOException {
        // Test DOC file rename
        testRenameFileType("doc_source", "doc_dest", FileType.DOC);
        
        // Test DIC file rename
        testRenameFileType("dic_source", "dic_dest", FileType.DIC);
        
        // Test POST file rename
        testRenameFileType("post_source", "post_dest", FileType.POST);
        
        // Test META file rename
        SegmentMetadata metadata = new SegmentMetadata(5, 1, 5);
        MetaFileWriter writer = storage.createMetaFileWriter("meta_source", metadata);
        writer.complete();
        
        storage.rename("meta_source.meta", "meta_dest.meta");
        assertFalse(storage.fileExists("meta_source.meta"));
        assertTrue(storage.fileExists("meta_dest.meta"));
    }

    private void testRenameFileType(String sourceName, String destName, FileType type) throws IOException {
        String sourceFile = sourceName + type.getExtension();
        String destFile = destName + type.getExtension();
        
        createTestFile(sourceName, type);
        storage.rename(sourceFile, destFile);
        
        assertFalse(storage.fileExists(sourceFile));
        assertTrue(storage.fileExists(destFile));
    }

    @Test
    void shouldThrowWhenRenamingNonExistentFile() {
        String sourceName = "nonexistent.doc";
        String destName = "new.doc";
        
        assertThrows(IOException.class, () -> storage.rename(sourceName, destName));
    }

    @Test
    void shouldThrowWhenRenamingToExistingFile() throws IOException {
        // Given
        createTestFile("source", FileType.DOC);
        createTestFile("dest", FileType.DOC);
        
        // When & Then
        assertThrows(IOException.class, () -> storage.rename("source.doc", "dest.doc"));
    }

    @Test
    void shouldThrowWhenRenamingWithInvalidNames() {
        // Test null source name
        assertThrows(IllegalArgumentException.class, () -> storage.rename(null, "dest.doc"));
        
        // Test empty source name
        assertThrows(IllegalArgumentException.class, () -> storage.rename("", "dest.doc"));
        assertThrows(IllegalArgumentException.class, () -> storage.rename("   ", "dest.doc"));
        
        // Test invalid characters in source name
        assertThrows(IllegalArgumentException.class, () -> storage.rename("../source.doc", "dest.doc"));
        assertThrows(IllegalArgumentException.class, () -> storage.rename("source/../doc", "dest.doc"));
        assertThrows(IllegalArgumentException.class, () -> storage.rename("source\\doc", "dest.doc"));
        
        // Test null dest name
        assertThrows(IllegalArgumentException.class, () -> storage.rename("source.doc", null));
        
        // Test empty dest name
        assertThrows(IllegalArgumentException.class, () -> storage.rename("source.doc", ""));
        assertThrows(IllegalArgumentException.class, () -> storage.rename("source.doc", "   "));
        
        // Test invalid characters in dest name
        assertThrows(IllegalArgumentException.class, () -> storage.rename("source.doc", "../dest.doc"));
        assertThrows(IllegalArgumentException.class, () -> storage.rename("source.doc", "dest/../doc"));
        assertThrows(IllegalArgumentException.class, () -> storage.rename("source.doc", "dest\\doc"));
    }

    @Test
    void shouldHandleRenameToSameName() throws IOException {
        // Given
        createTestFile("test", FileType.DOC);
        String fileName = "test.doc";
        
        // When & Then - should not throw and file should still exist
        assertDoesNotThrow(() -> storage.rename(fileName, fileName));
        assertTrue(storage.fileExists(fileName));
    }

    @Test
    void shouldThrowWhenClosedForRename() throws IOException {
        storage.close();
        assertThrows(IllegalStateException.class, () -> storage.rename("source.doc", "dest.doc"));
    }

    @Test
    void shouldPreserveFileContentAfterRename() throws IOException {
        // Given
        String sourceName = "source.doc";
        String destName = "dest.doc";
        createTestFile("source", FileType.DOC);
        
        // Get original file size
        long originalSize = storage.fileLength(sourceName);
        
        // When
        storage.rename(sourceName, destName);
        
        // Then
        assertEquals(originalSize, storage.fileLength(destName));
        assertEquals(originalSize, Files.size(tempDir.resolve(destName)));
    }

    @Test
    void shouldRenameFileAndUpdateListFiles() throws IOException {
        // Given
        createTestFile("file1", FileType.DOC);
        createTestFile("file2", FileType.DOC);
        String[] filesBefore = storage.listFiles();
        
        // When
        storage.rename("file1.doc", "renamed_file1.doc");
        
        // Then
        String[] filesAfter = storage.listFiles();
        assertEquals(filesBefore.length, filesAfter.length);
        
        assertFalse(containsFile(filesAfter, "file1.doc"));
        assertTrue(containsFile(filesAfter, "renamed_file1.doc"));
        assertTrue(containsFile(filesAfter, "file2.doc"));
    }
}