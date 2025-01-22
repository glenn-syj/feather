import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import storage.exception.InvalidHeaderException;
import storage.file.FeatherFileHeader;
import storage.file.FileType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

public class FeatherFileHeaderTest {

    @TempDir
    Path tempDir;

    @Test
    void constructor_ValidInput_Success() {
        // given & when
        FeatherFileHeader header = new FeatherFileHeader(FileType.DOC, 10);

        // then
        assertEquals(FileType.DOC, header.getFileType());
        assertEquals(10, header.getRecordCount());
        assertTrue(header.getTimestamp() > 0);
        assertEquals(FeatherFileHeader.HEADER_SIZE, header.getHeaderSize());
    }

    @Test
    void constructor_NullFileType_ThrowsException() {
        // given & when & then
        assertThrows(IllegalArgumentException.class,
                () -> new FeatherFileHeader(null, 10));
    }

    @Test
    void constructor_NegativeRecordCount_ThrowsException() {
        // given & when & then
        assertThrows(IllegalArgumentException.class,
                () -> new FeatherFileHeader(FileType.DOC, -1));
    }

    @Test
    void writeAndRead_ValidHeader_Success() throws Exception {
        // given
        FeatherFileHeader original = new FeatherFileHeader(FileType.DOC, 10);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        // when
        original.writeTo(dos);

        // then
        DataInputStream dis = new DataInputStream(
                new ByteArrayInputStream(baos.toByteArray()));
        FeatherFileHeader read = FeatherFileHeader.readFrom(dis);

        assertEquals(original.getFileType(), read.getFileType());
        assertEquals(original.getRecordCount(), read.getRecordCount());
        assertEquals(original.getHeaderSize(), read.getHeaderSize());
    }

    @Test
    void read_InvalidMagicNumber_ThrowsException() throws Exception {
        // given
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        // when
        dos.writeInt(0x12345678);  // Invalid magic number

        // then
        DataInputStream dis = new DataInputStream(
                new ByteArrayInputStream(baos.toByteArray()));
        assertThrows(InvalidHeaderException.class,
                () -> FeatherFileHeader.readFrom(dis));
    }

    @Test
    void read_InvalidVersion_ThrowsException() throws Exception {
        // given
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        // when
        dos.writeInt(FeatherFileHeader.MAGIC_NUMBER);
        dos.writeInt(0x00020000);  // Invalid version

        // then
        DataInputStream dis = new DataInputStream(
                new ByteArrayInputStream(baos.toByteArray()));
        assertThrows(InvalidHeaderException.class,
                () -> FeatherFileHeader.readFrom(dis));
    }

    @Test
    void fileChannel_WriteAndRead_Success() throws Exception {
        // given
        Path filePath = tempDir.resolve("test.doc");
        FeatherFileHeader original = new FeatherFileHeader(FileType.DOC, 10);

        // when - write
        try (FileChannel channel = FileChannel.open(filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE)) {
            original.writeTo(channel);
        }

        // when - read
        FeatherFileHeader read;
        try (FileChannel channel = FileChannel.open(filePath,
                StandardOpenOption.READ)) {
            read = FeatherFileHeader.readFrom(channel);
        }

        // then
        assertEquals(original.getFileType(), read.getFileType());
        assertEquals(original.getRecordCount(), read.getRecordCount());
        assertEquals(original.getHeaderSize(), read.getHeaderSize());
    }

    @Test
    void toString_ReturnsFormattedString() {
        // given
        FeatherFileHeader header = new FeatherFileHeader(FileType.DOC, 10);

        // when
        String result = header.toString();

        // then
        assertTrue(result.contains("DOC"));
        assertTrue(result.contains("10"));
        assertTrue(result.contains(String.valueOf(header.getTimestamp())));
        assertTrue(result.contains(String.valueOf(FeatherFileHeader.HEADER_SIZE)));
    }

    @Test
    void fileChannel_WriteAndRead_WithCorrectExtension() throws Exception {
        // given
        Path filePath = tempDir.resolve("test" + FileType.DOC.getExtension());
        FeatherFileHeader original = new FeatherFileHeader(FileType.DOC, 10);

        // when - write
        try (FileChannel channel = FileChannel.open(filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE)) {
            original.writeTo(channel);
        }

        // when - read
        FeatherFileHeader read;
        try (FileChannel channel = FileChannel.open(filePath,
                StandardOpenOption.READ)) {
            read = FeatherFileHeader.readFrom(channel);
        }

        // then
        assertEquals(original.getFileType(), read.getFileType());
        assertEquals(original.getRecordCount(), read.getRecordCount());
        assertEquals(original.getHeaderSize(), read.getHeaderSize());
        assertTrue(filePath.toString().endsWith(".doc"));
    }

    @Test
    void fromCode_WithInvalidCode_ThrowsException() {
        // given
        byte invalidCode = (byte) 0xFF;

        // when & then
        assertThrows(IllegalArgumentException.class,
                () -> FileType.fromCode(invalidCode));
    }
}