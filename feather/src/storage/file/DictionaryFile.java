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
    private static final int PREFIX_LENGTH = 8;
    private long termIndexPosition;
    private long termRecordsPosition;
    private final Map<Term, Long> termPositions;

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
        writeInt(fieldBytes.length);
        writeBytes(ByteBuffer.wrap(fieldBytes));

        byte[] textBytes = term.getText().getBytes(StandardCharsets.UTF_8);
        writeInt(textBytes.length);
        writeBytes(ByteBuffer.wrap(textBytes));

        writeInt(term.getDocumentFrequency());
        writeLong(postingPosition);

        termRecordsPosition = getPosition();
        termPositions.put(term, recordPosition);
    }

    public void writeTermIndex() throws IOException {

        termIndexPosition = termRecordsPosition;
        seek(termIndexPosition);

        writeInt(termPositions.size());

        for (Map.Entry<Term, Long> entry : termPositions.entrySet()) {
            Term term = entry.getKey();
            long recordPosition = entry.getValue();
            writeTermIndexEntry(term, recordPosition);
        }
    }

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
        writeInt(fieldBytes.length);
        writeBytes(ByteBuffer.wrap(fieldBytes));

        String prefix = getPrefixString(term.getText());
        byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
        writeInt(prefixBytes.length);
        writeBytes(ByteBuffer.wrap(prefixBytes));

        writeLong(recordPosition);
    }

    private String getPrefixString(String text) {
        return text.length() <= PREFIX_LENGTH ? text : text.substring(0, PREFIX_LENGTH);
    }

    private TermIndexEntry readTermIndexEntry() throws IOException {

        int fieldLength = readInt();
        String field = StandardCharsets.UTF_8.decode(readBytes(fieldLength)).toString();

        int prefixLength = readInt();
        String prefix = StandardCharsets.UTF_8.decode(readBytes(prefixLength)).toString();

        long recordPosition = readLong();

        TermIndexEntry entry = new TermIndexEntry();
        entry.field = field;
        entry.text = prefix;
        entry.recordPosition = recordPosition;
        return entry;
    }

    private Term readTermRecord() throws IOException {
        int fieldLength = readInt();
        String field = StandardCharsets.UTF_8.decode(readBytes(fieldLength)).toString();

        int textLength = readInt();
        String text = StandardCharsets.UTF_8.decode(readBytes(textLength)).toString();

        int docFreq = readInt();
        long postingPosition = readLong();

        return new Term(field, text, docFreq, postingPosition);
    }

    private int compareTerms(String field, String text, TermIndexEntry indexEntry) {
        int cmp = field.compareTo(indexEntry.field);
        if (cmp != 0) return cmp;

        String prefix = getPrefixString(text);
        return prefix.compareTo(indexEntry.text);
    }

    private static class TermIndexEntry {
        String field;
        String text;
        long recordPosition;
    }
}
