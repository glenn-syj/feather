package core.index;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

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

    public void flush() throws IOException {
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

        int docCount = documentBuffer.size();
        int minDocId = Integer.MAX_VALUE;
        int maxDocId = Integer.MIN_VALUE;

        // Process documents one by one, and their token streams directly
        for (Document doc : documentBuffer) {
            int docId = doc.getId();

            minDocId = Math.min(minDocId, docId);
            maxDocId = Math.max(maxDocId, docId);

            Map<String, Object> fields = doc.getFields();

            for (Map.Entry<String, Object> entry : fields.entrySet()) {
                String fieldName = entry.getKey();
                Object fieldValue = entry.getValue();

                if (fieldValue instanceof String) {
                    // TODO: Differentiate between TEXT (analyzed) and KEYWORD (non-analyzed) fields.
                    // This will be driven by a schema/mapping system in the future.
                    // For now, all string fields are treated as TEXT, intentionally.
                    boolean isAnalyzed = true;

                    if (isAnalyzed) {
                        // TEXT field: Analyzed for full-text search.
                        try (Stream<FeatherToken> tokenStream = analyzer.analyze((String) fieldValue)) {
                            tokenStream.forEach(token -> {
                                String term = token.term();
                                String fullTerm = fieldName + ":" + term;

                                postingLists
                                    .computeIfAbsent(fullTerm, k -> new HashMap<>())
                                    .computeIfAbsent(docId, k -> new ArrayList<>())
                                    .add(token.startOffset()); // Using startOffset as position.
                            });
                        }
                    } else {
                        // KEYWORD field: Not analyzed, treated as a single term for exact matches.
                        String term = (String) fieldValue;
                        String fullTerm = fieldName + ":" + term;
                        postingLists
                            .computeIfAbsent(fullTerm, k -> new HashMap<>())
                            .computeIfAbsent(docId, k -> new ArrayList<>())
                            .add(0); // For non-analyzed fields, position can be 0.
                    }
                }
                // TODO: Handle other field types (Numeric, Binary) if they need analysis or special processing
            }
        }

        System.out.println("Built in-memory posting lists for " + postingLists.size() + " unique terms.");

        DocumentFileWriter docWriter = null;
        PostingFileWriter postWriter = null;
        DictionaryFileWriter dicWriter = null;
        MetaFileWriter metaWriter = null;

        try {
            docWriter = (DocumentFileWriter) storage.createFileWriter(segmentName, FileType.DOC);
            postWriter = (PostingFileWriter) storage.createFileWriter(segmentName, FileType.POST);
            dicWriter = (DictionaryFileWriter) storage.createFileWriter(segmentName, FileType.DIC);

            // Writes documents.
            for (Document doc : documentBuffer) {
                docWriter.writeDocument(doc);
            }
            System.out.println("Wrote " + docCount + " documents to " + segmentName + FileType.DOC.getExtension());

            // Writes postings and prepares dictionary.
            List<Term> terms = new ArrayList<>();
            for (Map.Entry<String, Map<Integer, List<Integer>>> entry : postingLists.entrySet()) {
                String fullTerm = entry.getKey();
                Map<Integer, List<Integer>> postingsData = entry.getValue();

                List<Posting> postings = new ArrayList<>();
                for (Map.Entry<Integer, List<Integer>> postingEntry : postingsData.entrySet()) {
                    int docId = postingEntry.getKey();
                    int[] positions = postingEntry.getValue().stream().mapToInt(i -> i).toArray();
                    postings.add(new Posting(docId, positions.length, positions));
                }

                long postingPosition = postWriter.writePostingList(postings);

                int separatorIndex = fullTerm.indexOf(":");
                String fieldName = fullTerm.substring(0, separatorIndex);
                String termText = fullTerm.substring(separatorIndex + 1);

                terms.add(new Term(fieldName, termText, postings.size(), postingPosition));
            }
            System.out.println("Wrote " + terms.size() + " posting lists to " + segmentName + FileType.POST.getExtension());

            // Write dictionary.
            for (Term term : terms) {
                dicWriter.addTermRecord(term);
            }
            System.out.println("Added " + terms.size() + " terms to dictionary writer for " + segmentName);


            // Finalize segment files with complete()
            DocumentFile docFile = docWriter.complete();
            PostingFile postFile = postWriter.complete();
            DictionaryFile dicFile = dicWriter.complete();
            System.out.println("Finalized .doc, .post, and .dic files for " + segmentName);

            // Writing Metadata after finalizing segment files
            SegmentMetadata metadata = new SegmentMetadata(docCount, minDocId, maxDocId);
            metaWriter = storage.createMetaFileWriter(segmentName, metadata);
            MetaFile metaFile = metaWriter.complete();
            System.out.println("Wrote metadata to " + segmentName + FileType.META.getExtension());

            // Create an in-memory representation of the new segment and add it to the writer's state.
            SegmentInfo newSegment = new SegmentInfo(segmentName, System.currentTimeMillis(), docCount, minDocId, maxDocId);
            
            // Calculate and set the size in bytes for the new segment.
            // NOTE: This one is for further MERGE related feature.
            long segmentSize = docFile.size() + postFile.size() + dicFile.size() + metaFile.size();
            newSegment.setSizeInBytes(segmentSize);

            segmentsManager.addSegment(newSegment); // Use segmentsManager
            System.out.println("Created and registered new segment in-memory: " + newSegment);

        } catch (IOException e) {
            // Clean up any partially written files for this segment
            cleanupFailedSegment(segmentName);

            // Re-throw the original exception to the caller
            throw new IOException("Failed to flush segment " + segmentName, e);
        } finally {
            documentBuffer.clear();
            System.out.println("Document buffer cleared.");
            // The complete() method on writers should close them, but as a safeguard.
            try {
                if (docWriter != null) docWriter.close();
                if (postWriter != null) postWriter.close();
                if (dicWriter != null) dicWriter.close();
                if (metaWriter != null) metaWriter.close();
            } catch (IOException ex) {
                System.err.println("Error closing file writers: " + ex.getMessage());
            }
        }
    }

    private void cleanupFailedSegment(String segmentName) {
        System.err.println("Attempting to clean up failed segment: " + segmentName);
        for (FileType type : FileType.values()) {
            try {
                // deleteFile() safely handles cases where the file may not exist.
                storage.deleteFile(segmentName + type.getExtension());
            } catch (IOException ex) {
                System.err.println("Failed to delete cleanup file " + segmentName + type.getExtension() + ": " + ex.getMessage());
            }
        }
    }

    public void commit() throws IOException {
        flush(); // Ensure all buffered documents are written to segments

        // Persist segment metadata (segments_N file) using the Segments manager
        segmentsManager.write(storage);
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