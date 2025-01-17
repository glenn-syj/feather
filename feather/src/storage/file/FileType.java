package storage.file;

/**
 * Enumeration of file types used in the Feather search engine's segment files.
 *
 * <p>The file types are:</p>
 * <ul>
 *   <li>{@code DOC} (.doc) - Document storage files containing the actual document contents</li>
 *   <li>{@code DIC} (.dic) - Dictionary files storing term information and posting list references</li>
 *   <li>{@code POST} (.post) - Posting list files containing term occurrence information</li>
 *   <li>{@code META} (.meta) - Metadata files storing segment-level information</li>
 * </ul>
 *
 * <p>Each file type is identified by a unique single-byte code to ensure efficient
 * storage and validation during file operations.</p>
 *
 */
public enum FileType {

    // Explicitly uses a single byte
    DOC((byte) 0x01, ".doc"),
    DIC((byte) 0x02, ".dic"),
    POST((byte) 0x03, ".post"),
    META((byte) 0x04, ".meta");

    private final byte code;
    private final String extension;

    FileType(byte code, String extension) {
        this.code = code;
        this.extension = extension;
    }

    public byte getCode() {
        return code;
    }

    public String getExtension() {
        return extension;
    }

    public static FileType fromCode(byte code) {
        for (FileType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown file type code: " + code);
    }
}
