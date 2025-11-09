package core.index;

import storage.SegmentInfo;
import storage.Storage;
import storage.FileSystemStorage;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

/*
    Segments is
 */
public class Segments {
    public static final String SEGMENTS_GEN = "segments.gen";

    private final List<SegmentInfo> segments;
    private long generation; // The version of the segments file (the _N in segments_N)

    public Segments() {
        this.segments = new ArrayList<>();
        this.generation = -1;
    }

    private Segments(long generation, List<SegmentInfo> segments) {
        this.generation = generation;
        this.segments = segments;
    }

    public List<SegmentInfo> getSegments() {
        return new ArrayList<>(segments);
    }

    public void addSegment(SegmentInfo segment) {
        this.segments.add(segment);
    }

    public int size() {
        return segments.size();
    }

    /**
     * Writes the current list of segments to a new segments file.
     * @param storage The storage to write to.
     * @throws IOException If an I/O error occurs.
     */
    public void write(Storage storage) throws IOException {
        long nextGeneration = generation + 1;
        String segmentsFileName = "segments_" + nextGeneration;
        Path directory = ((FileSystemStorage) storage).getRootPath();
        Path tempSegmentsFile = Files.createTempFile(directory, "pending_segments_", ".tmp");

        // 1. Serialize segments list to a byte array
        byte[] data = serializeSegments(nextGeneration);

        // 2. Write data to a temporary file
        Files.write(tempSegmentsFile, data);

        // 3. Atomically rename the temporary file to the new segments file
        Files.move(tempSegmentsFile, directory.resolve(segmentsFileName), StandardCopyOption.ATOMIC_MOVE);

        // 4. Atomically update the generation file
        Path tempGenFile = Files.createTempFile(directory, "pending_gen_", ".tmp");
        Files.writeString(tempGenFile, String.valueOf(nextGeneration));
        Files.move(tempGenFile, directory.resolve(SEGMENTS_GEN), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        System.out.println("Successfully wrote " + segmentsFileName + " and updated " + SEGMENTS_GEN);

        // 5. TODO: Clean up old, unreferenced segments_N files

        this.generation = nextGeneration;
    }

    private byte[] serializeSegments(long currentGeneration) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeLong(currentGeneration); // Write the generation number
            oos.writeInt(this.segments.size());
            for (SegmentInfo segment : segments) {
                oos.writeObject(segment);
            }
        }
        return baos.toByteArray();
    }

    /**
     * Reads the most recent segments file from the storage.
     * @param storage The storage to read from.
     * @throws IOException If an I/O error occurs.
     */
    public static Segments readLatest(Storage storage) throws IOException {
        Path directory = ((FileSystemStorage) storage).getRootPath();
        Path genFile = directory.resolve(SEGMENTS_GEN);

        if (!Files.exists(genFile)) {
            // No segments file yet, this is a new index
            return new Segments();
        }

        long latestGeneration = Long.parseLong(Files.readString(genFile));
        String segmentsFileName = "segments_" + latestGeneration;
        Path segmentsFile = directory.resolve(segmentsFileName);

        if (!Files.exists(segmentsFile)) {
            throw new IOException("Segments file not found for generation: " + latestGeneration);
        }

        byte[] data = Files.readAllBytes(segmentsFile);
        return deserializeSegments(data);
    }

    private static Segments deserializeSegments(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            long generation = ois.readLong();
            int size = ois.readInt();
            List<SegmentInfo> segments = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                segments.add((SegmentInfo) ois.readObject());
            }
            return new Segments(generation, segments);
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize Segments file, class not found.", e);
        }
    }
}