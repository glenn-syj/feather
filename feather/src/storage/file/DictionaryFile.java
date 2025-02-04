package storage.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;

/*
    TODO: separate the term indexing logic field by field (String, Numeric)
    TODO: add a exact keyword match feature
 */
public class DictionaryFile extends SegmentFile {
    private static final int INDEX_BLOCK_SIZE = 128;
    private static final int PREFIX_LENGTH = 8;
    private long termIndexPosition;
    private long termRecordsPosition;
    private long blockDataPosition;
    private final Map<Term, Long> termPositions;

    private int blockCount;
    private long[] blockOffsets;

    public DictionaryFile(FileChannel channel, int bufferSize, FeatherFileHeader header) throws IOException {
        super(channel, bufferSize, header);
        this.termRecordsPosition = FeatherFileHeader.HEADER_SIZE;
        this.termPositions = new TreeMap<>(); // TreeMap to maintain the order of keys
    }

    @Override
    protected FileType getFileType() {
        return FileType.DIC;
    }

    public void writeTermRecord(Term term, long postingPosition) throws IOException {
        seek(termRecordsPosition);
        long recordPosition = termRecordsPosition;

        byte[] fieldBytes = term.getField().getBytes(StandardCharsets.UTF_8);
        writeShort((short) fieldBytes.length);
        writeBytes(ByteBuffer.wrap(fieldBytes));

        byte[] textBytes = term.getText().getBytes(StandardCharsets.UTF_8);
        writeShort((short) textBytes.length);
        writeBytes(ByteBuffer.wrap(textBytes));

        writeInt(term.getDocumentFrequency());
        writeLong(postingPosition);

        termRecordsPosition = getPosition();
        termPositions.put(term, recordPosition);
    }

    public void writeTermIndex() throws IOException {
        termIndexPosition = termRecordsPosition;
        seek(termIndexPosition);

        List<Map.Entry<Term, Long>> entries = new ArrayList<>(termPositions.entrySet());

        int blockCount = (entries.size() + INDEX_BLOCK_SIZE - 1) / INDEX_BLOCK_SIZE;
        writeInt(blockCount);

        long blockOffsetsPosition = position;
        for (int i = 0; i < blockCount; i++) {
            writeLong(0L); // assign temporary value
        }

        long[] blockOffsets = new long[blockCount];
        int blockIndex = 0;

        for (int i = 0; i < entries.size(); i += INDEX_BLOCK_SIZE) {
            Map.Entry<Term, Long> blockEntry = entries.get(i);
            blockOffsets[blockIndex++] = position - termIndexPosition;
            writeTermIndexEntry(blockEntry.getKey(), blockEntry.getValue());
        }

        long endPosition = position;
        seek(blockOffsetsPosition);
        for (long offset : blockOffsets) {
            writeLong(offset);
        }
        seek(endPosition);
    }

    /*
        TODO: modify the search logic based on the block-sized index
     */
    public Term findTerm(String field, String text) throws IOException {
        seek(termIndexPosition);
        int entryCount = readInt();

        int low = 0;
        int high = entryCount - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;

            seek(termIndexPosition + 4 + (mid * 8L));
            long entryPosition = readLong();

            seek(entryPosition);
            TermIndexEntry indexEntry = readTermIndexEntry();

            int cmp = compareTerms(field, text, indexEntry);
            if (cmp < 0) {
                high = mid - 1;
            } else if (cmp > 0) {
                low = mid + 1;
            } else {
                seek(indexEntry.recordPosition);
                return readTermRecord();
            }
        }

        return null;
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

    private String getPrefixString(String text) {
        return text.length() <= PREFIX_LENGTH ? text : text.substring(0, PREFIX_LENGTH);
    }

    private void readTermIndex() throws IOException {
        seek(termIndexPosition);

        blockCount = readInt();
        blockOffsets = new long[blockCount];
        for (int i = 0; i < blockCount; i++) {
            blockOffsets[i] = readLong();
        }
    }


    private TermIndexEntry readTermIndexEntry() throws IOException {

        short fieldLength = readShort();
        String field = StandardCharsets.UTF_8.decode(readBytes(fieldLength)).toString();

        short prefixLength = readShort();
        String prefix = StandardCharsets.UTF_8.decode(readBytes(prefixLength)).toString();

        long recordPosition = readLong();

        TermIndexEntry entry = new TermIndexEntry();
        entry.field = field;
        entry.text = prefix;
        entry.recordPosition = recordPosition;
        return entry;
    }

    private Term readTermRecord() throws IOException {
        short fieldLength = readShort();
        String field = StandardCharsets.UTF_8.decode(readBytes(fieldLength)).toString();

        short textLength = readShort();
        String text = StandardCharsets.UTF_8.decode(readBytes(textLength)).toString();

        int docFreq = readInt();
        long postingPosition = readLong();

        return new Term(field, text, docFreq, postingPosition);
    }

    private Term scanBlock(String field, String text) throws IOException {
        long startPosition = position;
        int termsScanned = 0;

        while (termsScanned < INDEX_BLOCK_SIZE && position < size()) {
            Term term = readTermRecord();
            int cmp = compareTerms(field, text, term);

            if (cmp == 0) {
                return term;
            }
            if (cmp < 0) {
                return null;
            }

            termsScanned++;
        }

        return null;
    }

    private int compareTerms(String field, String text, TermIndexEntry indexEntry) {
        int cmp = field.compareTo(indexEntry.field);
        if (cmp != 0) return cmp;

        String prefix = getPrefixString(text);
        return prefix.compareTo(indexEntry.text);
    }

    private int compareTerms(String field, String text, Term term) {
        int cmp = field.compareTo(term.getField());
        if (cmp != 0) return cmp;

        String prefix = getPrefixString(text);
        return prefix.compareTo(getPrefixString(term.getText()));
    }

    private static class TermIndexEntry {
        String field;
        String text;
        long recordPosition;
    }
}
