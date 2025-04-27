package storage.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class DocumentFile extends SegmentFile {
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    private static final byte TYPE_STRING = 1;
    private static final byte TYPE_NUMERIC = 2;
    private static final byte TYPE_BINARY = 3;

    public DocumentFile(FileChannel channel, int bufferSize) throws IOException {
        super(channel, bufferSize);
    }

    @Override
    protected FileType getFileType() {
        return FileType.DOC;
    }

    public Document readDocument() throws IOException {
        int id = readInt();
        int length = readInt();
        ByteBuffer content = readBytes(length);
        Document document = deserializeDocument(id, content);
        System.out.println(document);
        return document;
    }

    private Document deserializeDocument(int id, ByteBuffer buffer) {
        Document document = new Document(id);

        int fieldCount = buffer.getInt();

        for (int i = 0; i < fieldCount; i++) {
            readField(buffer, document);
        }

        return document;
    }

    private void readField(ByteBuffer buffer, Document document) {
        short nameLength = buffer.getShort();
        byte[] nameBytes = new byte[nameLength];
        buffer.get(nameBytes);
        String name = new String(nameBytes, CHARSET);

        byte type = buffer.get();
        Object value = readFieldValue(buffer, type);
        document.addField(name, value);
    }

    private Object readFieldValue(ByteBuffer buffer, byte type) {
        switch (type) {
            case TYPE_STRING:
                int strLength = buffer.getInt();
                byte[] strBytes = new byte[strLength];
                buffer.get(strBytes);
                return new String(strBytes, CHARSET);

            case TYPE_NUMERIC:
                return buffer.getLong();

            case TYPE_BINARY:
                int binLength = buffer.getInt();
                byte[] binBytes = new byte[binLength];
                buffer.get(binBytes);
                return binBytes;

            default:
                throw new IllegalArgumentException("Unknown field type: " + type);
        }
    }
}