package core.index;

import storage.Storage;
import storage.file.Document;
import core.analysis.FeatherAnalyzer;
import core.analysis.FeatherToken;
import storage.SegmentInfo;
import storage.Storage;
import storage.file.*;
import storage.writer.DictionaryFileWriter;
import storage.writer.DocumentFileWriter;
import storage.writer.MetaFileWriter;
import storage.writer.PostingFileWriter;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap; // Added import
import java.util.List;
import java.util.Map;

public class IndexWriter implements Closeable {

    private final Storage storage;
    private final IndexWriterConfig config;
    private final List<Document> documentBuffer;
    private final List<SegmentInfo> segments; // This will now be managed by segmentsManager
    private final Segments segmentsManager;
    private int segmentCounter;

    public IndexWriter(Storage storage, IndexWriterConfig config) throws IOException {
        this.storage = storage;
        this.config = config;
        this.documentBuffer = new ArrayList<>();
        
        // Load existing segments from the last commit point in storage.
        this.segmentsManager = Segments.readLatest(storage);
        this.segments = this.segmentsManager.getSegments(); // Initialize the list from the loaded segments

        // Initialize segmentCounter based on existing segments to avoid name collisions
        // Find the highest segment number from existing segments and increment it.
        int maxSegmentNum = -1;
        for (SegmentInfo si : segments) {
            String name = si.getName();
            if (name.startsWith("segment_")) {
                try {
                    int num = Integer.parseInt(name.substring("segment_".length()));
                    if (num > maxSegmentNum) {
                        maxSegmentNum = num;
                    }
                } catch (NumberFormatException e) {
                    // Ignore segments with non-numeric suffixes
                }
            }
        }
        this.segmentCounter = maxSegmentNum + 1;
    }

    public void addDocument(Document doc) throws IOException {
        documentBuffer.add(doc);
        if (documentBuffer.size() >= config.getMaxBufferedDocs()) {
            flush();
        }
    }

    private void flush() throws IOException {
        System.out.println("Flushing " + documentBuffer.size() + " documents.");

        if (documentBuffer.isEmpty()) {
            System.out.println("Document Buffer is empty, nothing to flush.");
            return;
        }

        FeatherAnalyzer analyzer = config.getAnalyzer();
        if (analyzer == null) {
            throw new IllegalStateException("FeatherAnalyzer is not configured in IndexWriterConfig.");
        }

        String segmentName = "segment_" + (segmentCounter++);
        System.out.println("Generated segment name: " + segmentName);

        // Map: term -> documentId -> list of positions
        Map<String, Map<Integer, List<Integer>>> postingLists = new HashMap<>();

        // Process documents one by one, and their token streams directly
        for (Document doc : documentBuffer) {
            int docId = doc.getId();
            Map<String, Object> fields = doc.getFields();

            for (Map.Entry<String, Object> entry : fields.entrySet()) {
                String fieldName = entry.getKey();
                Object fieldValue = entry.getValue();

                if (fieldValue instanceof String) {
                    // Get the stream and process it directly
                    analyzer.analyze((String) fieldValue).forEach(token -> {
                        String term = token.term();
                        // Combine field name and term for unique identification in the dictionary
                        String fullTerm = fieldName + ":" + term;

                        postingLists
                            .computeIfAbsent(fullTerm, k -> new HashMap<>())
                            .computeIfAbsent(docId, k -> new ArrayList<>())
                            .add(token.startOffset()); // Using startOffset as position for now
                    });
                }
                // TODO: Handle other field types (Numeric, Binary) if they need analysis or special processing
            }
        }

        System.out.println("Built in-memory posting lists for " + postingLists.size() + " unique terms.");


        /*
            TODO:Use SegmentFileWriters to write .doc, .post, .dic, .meta files.
            TODO:Create and register a new SegmentInfo.
            TODO:Clear the document buffer.
        */
        documentBuffer.clear();
    }

    public void commit() throws IOException {
        flush();
        // TODO: Persist segment metadata (segments_N file).
    }

    @Override
    public void close() throws IOException {
        try {
            commit();
        } finally {
            storage.close();
        }
    }
}