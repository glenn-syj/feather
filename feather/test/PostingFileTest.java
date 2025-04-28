import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import storage.file.FileType;
import storage.file.Posting;
import storage.file.PostingFile;
import storage.writer.PostingFileWriter;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class PostingFileTest {
    private static final int BUFFER_SIZE = 8192;

    @TempDir
    Path tempDir;

    private Path filePath;
    private PostingFile file;
    private PostingFileWriter writer;

    @BeforeEach
    void setUp() throws IOException {
        filePath = tempDir.resolve("test.post");
        writer = new PostingFileWriter(filePath, BUFFER_SIZE);
    }

    @AfterEach
    void tearDown() throws IOException {
        writer = null;
        file = null;
    }

    @Test
    void shouldReadWritePostingList() throws IOException {
        // Given
        List<Posting> postings = Arrays.asList(
                new Posting(1, 2, new int[]{0, 3}),
                new Posting(3, 1, new int[]{1}),
                new Posting(5, 3, new int[]{2, 4, 7})
        );

        // When
        long position = writer.writePostingList(postings);
        file = writer.complete();
        
        file.seekToPostingList(position);
        List<Posting> readPostings = file.readPostingList();

        // Then
        assertEquals(postings, readPostings);
    }

    @Test
    void shouldReadFromPosition() throws IOException {
        // Given
        List<Posting> postings = Arrays.asList(
                new Posting(1, 1, new int[]{0}),
                new Posting(2, 1, new int[]{1})
        );

        // When
        long position = writer.writePostingList(postings);
        file = writer.complete();

        // Then
        file.seekToPostingList(position);
        assertEquals(postings, file.readPostingList());
    }

    @Test
    void shouldHandleEmptyPostingList() throws IOException {
        // Given
        List<Posting> emptyPostings = Arrays.asList();

        // When
        long position = writer.writePostingList(emptyPostings);
        file = writer.complete();
        
        file.seekToPostingList(position);
        List<Posting> readPostings = file.readPostingList();

        // Then
        assertTrue(readPostings.isEmpty());
    }

    @Test
    void shouldPreserveHeaderInformation() throws IOException {
        // Given
        List<Posting> postings = Arrays.asList(
                new Posting(1, 1, new int[]{0}),
                new Posting(2, 1, new int[]{1})
        );
        
        // When
        writer.writePostingList(postings);
        file = writer.complete();
        
        FileType fileType = file.getHeaderFileType();
        int recordCount = file.getHeaderRecordCount();

        // Then
        assertEquals(FileType.POST, fileType);
        assertEquals(1, recordCount); // One posting list was written
    }

    @Test
    void shouldThrowExceptionForInvalidSeek() throws IOException {
        // Given
        writer.writePostingList(Arrays.asList(new Posting(1, 1, new int[]{0})));
        file = writer.complete();
        
        // When & Then
        assertThrows(IllegalArgumentException.class, () ->
                file.seekToPostingList(0)); // Less than header size
    }

    @Test
    void shouldHandleDeltaEncoding() throws IOException {
        // Given
        List<Posting> postings = Arrays.asList(
                new Posting(100, 1, new int[]{10}),
                new Posting(200, 1, new int[]{20}),
                new Posting(300, 1, new int[]{30})
        );

        // When
        long position = writer.writePostingList(postings);
        file = writer.complete();
        
        file.seekToPostingList(position);
        List<Posting> readPostings = file.readPostingList();

        // Then
        assertEquals(postings, readPostings);
    }
    
    @Test
    void verifyFileAfterReopen() throws IOException {
        // Given
        List<Posting> postings = Arrays.asList(
                new Posting(1, 2, new int[]{0, 3}),
                new Posting(3, 1, new int[]{1})
        );
        long position = writer.writePostingList(postings);
        file = writer.complete();
        file.close();
        
        // When - Reopen the file
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ);
             PostingFile reopenedFile = new PostingFile(channel, BUFFER_SIZE)) {
            
            // Then
            reopenedFile.seekToPostingList(position);
            List<Posting> readPostings = reopenedFile.readPostingList();
            assertEquals(postings, readPostings);
        }
    }
    
    @Test
    void shouldWriteMultiplePostingLists() throws IOException {
        // Given
        List<Posting> postings1 = Arrays.asList(
                new Posting(1, 1, new int[]{0}),
                new Posting(2, 1, new int[]{1})
        );
        
        List<Posting> postings2 = Arrays.asList(
                new Posting(3, 1, new int[]{2}),
                new Posting(4, 1, new int[]{3})
        );

        // When
        long position1 = writer.writePostingList(postings1);
        long position2 = writer.writePostingList(postings2);
        file = writer.complete();

        // Then
        file.seekToPostingList(position1);
        assertEquals(postings1, file.readPostingList());
        
        file.seekToPostingList(position2);
        assertEquals(postings2, file.readPostingList());
        
        // Verify header record count is 2
        assertEquals(2, file.getHeaderRecordCount());
    }
}