import org.junit.jupiter.api.*;
import storage.FileManager;

import java.io.IOException;
import java.nio.file.*;
import java.util.Arrays;
import java.util.List;

public class FileManagerTest {
    private FileManager fileManager;
    private static final String TEST_INDEX = "test";

    @BeforeEach
    void setUp() {
        fileManager = new FileManager();
    }

    @AfterEach
    void cleanup() throws IOException {
        // deletes file after the test
        Files.deleteIfExists(Paths.get("data", TEST_INDEX + ".dat"));
    }

    @Test
    void saveAndLoadDocuments() throws IOException {
        // Given
        String content1 = "Hello, World!";
        String content2 = "Testing, 1,2,3";
        String content3 = "Final test";

        // When
        fileManager.saveDocument(TEST_INDEX, 1, content1);
        fileManager.saveDocument(TEST_INDEX, 2, content2);
        fileManager.saveDocument(TEST_INDEX, 3, content3);

        List<Integer> idsToLoad = Arrays.asList(1, 3);
        List<String> loadedDocs = fileManager.loadDocuments(TEST_INDEX, idsToLoad);

        // Then
        Assertions.assertEquals(2, loadedDocs.size());
        Assertions.assertTrue(loadedDocs.contains(content1));
        Assertions.assertTrue(loadedDocs.contains(content3));
        Assertions.assertFalse(loadedDocs.contains(content2));
    }

    @Test
    void loadNonExistentDocuments() {
        // Given
        List<Integer> nonExistentIds = Arrays.asList(100, 200);

        // When
        List<String> loadedDocs = fileManager.loadDocuments(TEST_INDEX, nonExistentIds);

        // Then
        Assertions.assertTrue(loadedDocs.isEmpty());
    }

    @Test
    void saveAndLoadDocumentWithSpecialCharacters() throws IOException {
        // Given
        String contentWithSpecialChars = "Hello:World!:With:Colons";

        // When
        fileManager.saveDocument(TEST_INDEX, 1, contentWithSpecialChars);
        List<String> loadedDocs = fileManager.loadDocuments(TEST_INDEX, Arrays.asList(1));

        // Then
        Assertions.assertEquals(1, loadedDocs.size());
        Assertions.assertEquals(contentWithSpecialChars, loadedDocs.get(0));
    }
}
