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

    public void writeTermRecord(Term term) throws IOException {
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

    public void writeTermIndex() throws IOException {
        System.out.println("Starting writeTermIndex");
        termIndexPosition = termRecordsPosition;
        System.out.println("termIndexPosition: " + termIndexPosition);
        seek(termIndexPosition);
        System.out.println("Position after seek: " + position);

        List<Map.Entry<Term, Long>> entries = new ArrayList<>(termPositions.entrySet());
        System.out.println("Number of entries: " + entries.size());

        blockCount = (entries.size() + INDEX_BLOCK_SIZE - 1) / INDEX_BLOCK_SIZE;
        System.out.println("Calculated blockCount: " + blockCount);
        writeInt(blockCount);

        long blockOffsetsPosition = position;
        System.out.println("blockOffsetsPosition: " + blockOffsetsPosition);
        for (int i = 0; i < blockCount; i++) {
            writeLong(0L);
        }

        blockOffsets = new long[blockCount];
        int blockIndex = 0;

        for (int i = 0; i < entries.size(); i += INDEX_BLOCK_SIZE) {
            Map.Entry<Term, Long> blockEntry = entries.get(i);
            blockOffsets[blockIndex] = position - termIndexPosition;
            System.out.println("Block " + blockIndex + " offset: " + blockOffsets[blockIndex]);
            writeTermIndexEntry(blockEntry.getKey(), blockEntry.getValue());
            blockIndex++;
        }

        long endPosition = position;
        System.out.println("endPosition before writing offsets: " + endPosition);
        seek(blockOffsetsPosition);
        System.out.println("Position after seeking to blockOffsetsPosition: " + position);
        for (long offset : blockOffsets) {
            writeLong(offset);
        }
        seek(endPosition);
        System.out.println("Final position: " + position);
    }

    public Term findTerm(String field, String text) throws IOException {
        int low = 0;
        int high = blockCount - 1;

        TermIndexEntry currentEntry, nextEntry;
        while (low <= high) {
            int mid = (low + high) >>> 1;

            seek(termIndexPosition + blockOffsets[mid]);
            currentEntry = readTermIndexEntry();

            int cmp = compareTerms(field, text, currentEntry);
            if (cmp < 0) {
                high = mid - 1;
            } else {
                if (mid == blockCount - 1) {
                    return scanBlock(field, text, mid);
                }

                seek(termIndexPosition + blockOffsets[mid + 1]);
                nextEntry = readTermIndexEntry();

                if (compareTerms(field, text, nextEntry) < 0) {
                    return scanBlock(field, text, mid);
                }
                low = mid + 1;
            }
        }

        return null;
    }

//    public Term findTerm(String field, String text) throws IOException {
//        System.out.println("Finding term: field=" + field + ", text=" + text);
//        System.out.println("blockCount: " + blockCount);
//        System.out.println("termIndexPosition: " + termIndexPosition);
//
//        int low = 0;
//        int high = blockCount - 1;
//
//        TermIndexEntry currentEntry, nextEntry;
//        while (low <= high) {
//            int mid = (low + high) >>> 1;
//
//            seek(termIndexPosition + blockOffsets[mid]);
//            System.out.println(Arrays.toString(blockOffsets));
//            currentEntry = readTermIndexEntry();
//
//            int cmp = compareTerms(field, text, currentEntry);
//            System.out.println("mid: " + mid + " cmp: " + cmp);
//            if (cmp < 0) {
//                high = mid - 1;
//            } else {
//                if (mid == blockCount - 1) {
//                    System.out.println("mid == blockCount - 1");
//                    return scanBlock(field, text, mid);
//                }
//
//                seek(termIndexPosition + blockOffsets[mid + 1]);
//                nextEntry = readTermIndexEntry();
//
//                if (compareTerms(field, text, nextEntry) < 0) {
//                    return scanBlock(field, text, mid);
//                }
//                low = mid + 1;
//            }
//        }
//
//        return null;
//    }

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
        blockDataPosition = position;
    }


    private TermIndexEntry readTermIndexEntry() throws IOException {

        System.out.println("Current position before reading: " + position);

        short fieldLength = readShort();
        String field = StandardCharsets.UTF_8.decode(readBytes(fieldLength)).toString();
        System.out.println("TIE field: " + field);


        short prefixLength = readShort();
        String prefix = StandardCharsets.UTF_8.decode(readBytes(prefixLength)).toString();
        System.out.println("TIE prefix: " + prefix);

        long recordPosition = readLong();

        TermIndexEntry entry = new TermIndexEntry();
        entry.field = field;
        entry.text = prefix;
        entry.recordPosition = recordPosition;
        System.out.println("after readTIE position: " + position);
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

    private Term scanBlock(String field, String text, int blockIndex) throws IOException {
        seek(termIndexPosition + blockOffsets[blockIndex]);
        TermIndexEntry entry = readTermIndexEntry();
        seek(entry.recordPosition);
        int termsScanned = 0;

        while (termsScanned < INDEX_BLOCK_SIZE && position < termIndexPosition) {
            System.out.println("termsScanned: " + termsScanned + " position: " + position);
            Term term = readTermRecord();
            System.out.println("Term in the scanned block: text=" + term.getText() + " field=" + term.getField());
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
