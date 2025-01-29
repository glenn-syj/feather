package storage.file;

import storage.exception.InvalidHeaderException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
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

    public DocumentFile(FileChannel channel, int bufferSize, FeatherFileHeader header) throws IOException {
        super(channel, bufferSize, header);
    }

    @Override
    protected FileType getFileType() {
        return FileType.DOC;
    }

    public void writeDocument(Document document) throws IOException {
        validateDocument(document);
        channel.position(channel.size());
        writeInt(document.getId());
        ByteBuffer content = serializeDocument(document);
        writeInt(content.remaining());
        writeBytes(content);
    }

    public Document readDocument() throws IOException {
        int id = readInt();
        int length = readInt();
        ByteBuffer content = readBytes(length);

        return deserializeDocument(id, content);
    }

    public void seekToContent() throws IOException {
        seek(FeatherFileHeader.HEADER_SIZE);
    }

    private void validateDocument(Document document) {
        if (document == null || document.getId() < 0) {
            throw new IllegalArgumentException("Invalid document");
        }
    }

    private ByteBuffer serializeDocument(Document document) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(calculateBufferSize(document));
        Map<String, Object> fields = document.getFields();

        buffer.putInt(fields.size());

        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            writeField(buffer, entry.getKey(), entry.getValue());
        }

        buffer.flip();
        return buffer;
    }

    private void writeField(ByteBuffer buffer, String name, Object value) throws IOException {
        byte[] nameBytes = name.getBytes(CHARSET);
        buffer.putShort((short) nameBytes.length);
        buffer.put(nameBytes);

        if (value instanceof String strValue) {
            byte[] bytes = strValue.getBytes(CHARSET);
            buffer.put(TYPE_STRING);
            buffer.putInt(bytes.length);
            buffer.put(bytes);
        } else if (value instanceof Number) {
            buffer.put(TYPE_NUMERIC);
            buffer.putLong(((Number) value).longValue());
        } else if (value instanceof byte[] bytes) {
            buffer.put(TYPE_BINARY);
            buffer.putInt(bytes.length);
            buffer.put(bytes);
        } else {
            throw new IllegalArgumentException("this type of value cannot be documented");
        }

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

    private int calculateBufferSize(Document document) {

        int size = 4;
        for (Map.Entry<String, Object> entry : document.getFields().entrySet()) {
            size += 2 + entry.getKey().getBytes(CHARSET).length;
            size += 1;
            Object value = entry.getValue();
            if (value instanceof String) {
                size += 4 + ((String) value).getBytes(CHARSET).length;
            } else if (value instanceof Number) {
                size += 8;
            } else if (value instanceof byte[]) {
                size += 4 + ((byte[]) value).length;
            }
        }
        return size;
    }
}