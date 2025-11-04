package core.index;

import storage.Storage;
import storage.file.Document;
import core.analysis.FeatherAnalyzer;
import core.analysis.FeatherToken;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap; // Added import
import java.util.List;
import java.util.Map;

public class IndexWriter {

    private final Storage storage;
    private final IndexWriterConfig config;
    private final List<Document> documentBuffer;
    private int segmentCounter = 0; // Added segment counter

    public IndexWriter(Storage storage, IndexWriterConfig config) {
        this.storage = storage;
        this.config = config;
        this.documentBuffer = new ArrayList<>();
    }

    public void addDocument(Document doc) throws IOException {
        documentBuffer.add(doc);
        if (documentBuffer.size() >= config.getMaxBufferedDocs()) {
            flush();
        }
    }

    private void flush() throws IOException {
        System.out.println("Flushing " + documentBuffer.size() + " documents.");

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

    public void close() throws IOException {
        try {
            commit();
        } finally {
            storage.close();
        }
    }
}