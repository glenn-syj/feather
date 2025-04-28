package storage.writer;

import storage.file.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class DictionaryFileWriter extends SegmentFileWriter {
    private static final int INDEX_BLOCK_SIZE = 128;
    private static final int PREFIX_LENGTH = 8;
    private final long metadataPosition = FeatherFileHeader.HEADER_SIZE;
    private long termIndexPosition;
    private long termRecordsPosition;
    private int blockCount;
    private final Map<Term, Long> termPositions;
    private final List<Term> termsCache;

    public DictionaryFileWriter(Path path, int bufferSize) throws IOException {
        super(path, bufferSize);
        this.termRecordsPosition = FeatherFileHeader.HEADER_SIZE + 8 + 8 + 4;
        this.termPositions = new TreeMap<>(); // TreeMap to maintain the order of keys
        this.termsCache = new ArrayList<>();
    }

    public void addTermRecord(Term term) {
        termsCache.add(term);
    }

    private void writeTermRecord(Term term) throws IOException {
        long recordPosition = termRecordsPosition;

        byte[] fieldBytes = term.getField().getBytes(StandardCharsets.UTF_8);
        writeShort((short) fieldBytes.length);
        writeBytes(ByteBuffer.wrap(fieldBytes));

        byte[] textBytes = term.getText().getBytes(StandardCharsets.UTF_8);
        writeShort((short) textBytes.length);
        writeBytes(ByteBuffer.wrap(textBytes));

        writeInt(term.getDocumentFrequency());
        writeLong(term.getPostingPosition());

        termPositions.put(term, recordPosition);
        termRecordsPosition = position;
    }

    private void writeTermRecords() throws IOException {
        position = termRecordsPosition;

        for (Term term : termsCache) {
            System.out.println(term);
            writeTermRecord(term);
        }

        termIndexPosition = position;
    }

    private void writeTermIndex() throws IOException {
        position = termIndexPosition;

        List<Map.Entry<Term, Long>> entries = new ArrayList<>(termPositions.entrySet());
        blockCount = (entries.size() + INDEX_BLOCK_SIZE - 1) / INDEX_BLOCK_SIZE;
        writeInt(blockCount);

        long blockOffsetsPosition = position;
        for (int i = 0; i < blockCount; i++) {
            writeLong(0L);
        }

        long[] blockOffsets = new long[blockCount];
        int blockIndex = 0;

        for (int i = 0; i < entries.size(); i += INDEX_BLOCK_SIZE) {
            Map.Entry<Term, Long> blockEntry = entries.get(i);
            blockOffsets[blockIndex] = position - termIndexPosition;
            writeTermIndexEntry(blockEntry.getKey(), blockEntry.getValue());
            blockIndex++;
        }

        long endPosition = position;
        position = blockOffsetsPosition;
        for (long offset : blockOffsets) {
            writeLong(offset);
        }
        position = endPosition;
    }

    private void writeTermIndexEntry(Term term, long recordPosition) throws IOException {
        byte[] fieldBytes = term.getField().getBytes(StandardCharsets.UTF_8);
        if (fieldBytes.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Field name too long: " + term.getField());
        }
        writeShort((short) fieldBytes.length);
        writeBytes(ByteBuffer.wrap(fieldBytes));

        String prefix = getPrefixString(term.getText());
        byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
        if (prefixBytes.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Prefix too long: " + prefix);
        }
        writeShort((short) prefixBytes.length);
        writeBytes(ByteBuffer.wrap(prefixBytes));

        writeLong(recordPosition);
    }

    private void writeMetadata() throws IOException {
        position = FeatherFileHeader.HEADER_SIZE;
        writeLong(termRecordsPosition);
        writeLong(termIndexPosition);
        writeInt(blockCount);
    }

    private String getPrefixString(String text) {
        return text.length() <= PREFIX_LENGTH ? text : text.substring(0, PREFIX_LENGTH);
    }

    @Override
    public DictionaryFile complete() throws IOException {
        Collections.sort(termsCache);

        // Update header with term count
        FeatherFileHeader header = new FeatherFileHeader(FileType.DIC, termsCache.size());
        writeHeader(header);

        long metadataStart = position;
        position += (8 + 8 + 4);  // termRecordsStart + indexStart + blockCount

        long termRecordsStart = position;
        writeTermRecords();
        long indexStart = position;

        writeTermIndex();

        position = metadataStart;
        writeLong(termRecordsStart);
        writeLong(indexStart);
        writeInt(blockCount);

        flush();
        channel.close();

        // Create and return the read-only file
        FileChannel readChannel = FileChannel.open(path, StandardOpenOption.READ);
        return new DictionaryFile(readChannel, bufferSize);
    }
} 