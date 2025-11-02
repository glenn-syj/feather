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

        List<Map<String, List<FeatherToken>>> analyzedDocumentTokens = new ArrayList<>();
        List<Integer> documentIds = new ArrayList<>();

        for (Document doc : documentBuffer) {
            documentIds.add(doc.getId());
            Map<String, Object> fields = doc.getFields();
            Map<String, List<FeatherToken>> docTokens = new java.util.HashMap<>();
            for (Map.Entry<String, Object> entry : fields.entrySet()) {
                String fieldName = entry.getKey();
                Object fieldValue = entry.getValue();

                if (fieldValue instanceof String) {
                    List<FeatherToken> tokens = analyzer.analyze((String) fieldValue);
                    docTokens.put(fieldName, tokens);
                }
                // TODO: Handle other field types (Numeric, Binary) if they need analysis or special processing
            }
            analyzedDocumentTokens.add(docTokens);
        }

        String segmentName = "segment_" + (segmentCounter++);
        System.out.println("Generated segment name: " + segmentName);

        // Map: term -> documentId -> list of positions
        Map<String, Map<Integer, List<Integer>>> postingLists = new HashMap<>();

        for (int i = 0; i < analyzedDocumentTokens.size(); i++) {
            int docId = documentIds.get(i);
            Map<String, List<FeatherToken>> docTokens = analyzedDocumentTokens.get(i);

            for (Map.Entry<String, List<FeatherToken>> fieldEntry : docTokens.entrySet()) {
                String fieldName = fieldEntry.getKey();
                List<FeatherToken> tokens = fieldEntry.getValue();

                for (FeatherToken token : tokens) {
                    String term = token.term();
                    // Combine field name and term for unique identification in the dictionary
                    String fullTerm = fieldName + ":" + term;

                    postingLists
                        .computeIfAbsent(fullTerm, k -> new HashMap<>())
                        .computeIfAbsent(docId, k -> new ArrayList<>())
                        .add(token.startOffset()); // Using startOffset as position for now
                }
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