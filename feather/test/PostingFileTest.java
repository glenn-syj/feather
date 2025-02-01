import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import storage.file.FeatherFileHeader;
import storage.file.FileType;
import storage.file.Posting;
import storage.file.PostingFile;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class PostingFileTest {
    private static final int BUFFER_SIZE = 8192;

    @TempDir
    Path tempDir;

    private Path filePath;
    private FileChannel channel;
    private PostingFile postingFile;

    @BeforeEach
    void setUp() throws IOException {
        filePath = tempDir.resolve("test.post");
        channel = FileChannel.open(filePath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);
        FeatherFileHeader header = new FeatherFileHeader(FileType.POST, 10);
        postingFile = new PostingFile(channel, BUFFER_SIZE, header);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
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
        long position = postingFile.getPosition();
        postingFile.writePostingList(postings);
        postingFile.seekToPostingList(position);
        List<Posting> readPostings = postingFile.readPostingList();

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
        long position = postingFile.getPosition();
        postingFile.writePostingList(postings);

        // Then
        postingFile.seekToPostingList(position);
        assertEquals(postings, postingFile.readPostingList());
    }

    @Test
    void shouldHandleEmptyPostingList() throws IOException {
        // Given
        List<Posting> emptyPostings = Arrays.asList();

        // When
        long position = channel.size();
        postingFile.writePostingList(emptyPostings);
        postingFile.seekToPostingList(position);
        List<Posting> readPostings = postingFile.readPostingList();

        // Then
        assertTrue(readPostings.isEmpty());
    }

    @Test
    void shouldPreserveHeaderInformation() throws IOException {
        // When
        FileType fileType = postingFile.getHeaderFileType();
        int recordCount = postingFile.getHeaderRecordCount();

        // Then
        assertEquals(FileType.POST, fileType);
        assertEquals(10, recordCount);
    }

    @Test
    void shouldThrowExceptionForInvalidSeek() {
        // When & Then
        assertThrows(IllegalArgumentException.class, () ->
                postingFile.seekToPostingList(FeatherFileHeader.HEADER_SIZE - 1));
    }

    @Test
    void shouldThrowExceptionForNullPostingList() {
        // When & Then
        assertThrows(IllegalArgumentException.class, () ->
                postingFile.writePostingList(null));
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
        long position = postingFile.getPosition();
        postingFile.writePostingList(postings);
        postingFile.seekToPostingList(position);
        List<Posting> readPostings = postingFile.readPostingList();

        // Then
        assertEquals(postings, readPostings);
    }
}