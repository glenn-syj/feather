package storage.writer;

import storage.file.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

public class DocumentFileWriter extends SegmentFileWriter {
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    private static final byte TYPE_STRING = 1;
    private static final byte TYPE_NUMERIC = 2;
    private static final byte TYPE_BINARY = 3;
    private int documentCount = 0;

    public DocumentFileWriter(Path path, int bufferSize) throws IOException {
        super(path, bufferSize);
        
        // Write initial header
        FeatherFileHeader header = new FeatherFileHeader(FileType.DOC, 0);
        writeHeader(header);
    }

    public void writeDocument(Document document) throws IOException {
        validateDocument(document);
        writeInt(document.getId());
        ByteBuffer content = serializeDocument(document);
        writeInt(content.remaining());
        writeBytes(content);
        documentCount++;
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

    @Override
    public DocumentFile complete() throws IOException {
        // Update header with document count
        position = 0;
        FeatherFileHeader header = new FeatherFileHeader(FileType.DOC, documentCount);
        writeHeader(header);

        // Ensure all data is written to disk
        flush();

        // Close current write channel
        channel.close();

        // Create and return the read-only file
        FileChannel readChannel = FileChannel.open(path, StandardOpenOption.READ);
        return new DocumentFile(readChannel, bufferSize);
    }
} 