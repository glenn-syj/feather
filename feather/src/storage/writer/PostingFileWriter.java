package storage.writer;

import storage.file.*;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class PostingFileWriter extends SegmentFileWriter {
    private static final int MIN_BUFFER_SIZE = 4096; // 4KB
    private int postingListCount = 0;

    public PostingFileWriter(Path path, int bufferSize) throws IOException {
        super(path, validateBufferSize(bufferSize));
        
        // Write initial header
        FeatherFileHeader header = new FeatherFileHeader(FileType.POST, 0);
        writeHeader(header);
    }

    private static int validateBufferSize(int bufferSize) {
        if (bufferSize < MIN_BUFFER_SIZE) {
            throw new IllegalArgumentException(
                    "Buffer size must be at least " + MIN_BUFFER_SIZE + " bytes");
        }
        return bufferSize;
    }

    public long writePostingList(List<Posting> postings) throws IOException {
        if (postings == null) {
            throw new IllegalArgumentException("Postings list cannot be null");
        }

        long startPosition = position;
        postings.sort(Posting::compareTo);

        writeInt(postings.size());

        // Delta encoding for document IDs
        int prevDocId = 0;
        for (Posting posting : postings) {
            // Write delta-encoded document ID
            int deltaDocId = posting.getDocumentId() - prevDocId;
            writeInt(deltaDocId);
            prevDocId = posting.getDocumentId();

            // Write frequency
            writeInt(posting.getFrequency());

            // Write positions with delta encoding
            int[] positions = posting.getPositions();
            writeInt(positions.length);

            int prevPosition = 0;
            for (int position : positions) {
                int deltaPosition = position - prevPosition;
                writeInt(deltaPosition);
                prevPosition = position;
            }
        }
        
        postingListCount++;
        return startPosition;
    }

    @Override
    public PostingFile complete() throws IOException {
        // Update header with posting list count
        position = 0;
        FeatherFileHeader header = new FeatherFileHeader(FileType.POST, postingListCount);
        writeHeader(header);

        close();
        
        // Create and return the read-only file
        FileChannel readChannel = FileChannel.open(path, StandardOpenOption.READ);
        return new PostingFile(readChannel, bufferSize);
    }
} 