package storage.file;

import storage.exception.InvalidHeaderException;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class PostingFile extends SegmentFile {
    private static final int MIN_BUFFER_SIZE = 4096; // 4KB

    public PostingFile(FileChannel channel, int bufferSize) throws IOException {
        super(channel, validateBufferSize(bufferSize));
    }

    public PostingFile(FileChannel channel, int bufferSize, FeatherFileHeader header) throws IOException {
        super(channel, validateBufferSize(bufferSize), header);
        validateFileType(header);
    }

    private static int validateBufferSize(int bufferSize) {
        if (bufferSize < MIN_BUFFER_SIZE) {
            throw new IllegalArgumentException(
                    "Buffer size must be at least " + MIN_BUFFER_SIZE + " bytes");
        }
        return bufferSize;
    }

    private void validateFileType(FeatherFileHeader header) {
        if (header.getFileType() != FileType.POST) {
            throw new InvalidHeaderException(
                    "Invalid file type: expected POST but was " + header.getFileType());
        }
    }

    @Override
    protected FileType getFileType() {
        return FileType.POST;
    }

    public void writePostingList(List<Posting> postings) throws IOException {
        if (postings == null) {
            throw new IllegalArgumentException("Postings list cannot be null");
        }

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
        flush();
    }

    public List<Posting> readPostingList() throws IOException {
        int documentCount = readInt();
        List<Posting> postings = new ArrayList<>(documentCount);

        int prevDocId = 0;
        for (int i = 0; i < documentCount; i++) {
            int deltaDocId = readInt();
            int docId = prevDocId + deltaDocId;
            prevDocId = docId;

            // Read frequency
            int frequency = readInt();

            // Read and decode positions
            int positionCount = readInt();
            int[] positions = new int[positionCount];

            int prevPosition = 0;
            for (int j = 0; j < positionCount; j++) {
                int deltaPosition = readInt();
                positions[j] = prevPosition + deltaPosition;
                prevPosition = positions[j];
            }

            postings.add(new Posting(docId, frequency, positions));
        }

        return postings;
    }

    public void seekToPostingList(long position) throws IOException {
        if (position < FeatherFileHeader.HEADER_SIZE) {
            throw new IllegalArgumentException(
                    "Invalid position: " + position +
                            ". Position must be >= " + FeatherFileHeader.HEADER_SIZE);
        }

        seek(position);
    }

    public long getCurrentPosition() throws IOException {
        return channel.position();
    }
}