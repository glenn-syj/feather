package storage.file;

import java.util.Objects;

/*
    TODO: refactor the text with BytesRef based on the search feature of Lucene
 */
public class Term implements Comparable<Term> {

    // Later, search by field should be operated
    private final String field;
    private final String text;
    private final int documentFrequency;
    private final long postingPosition;
    private final TermStatistics stats;

    public Term(String field, String text, int documentFrequency, long postingPosition) {
        this.field = field;
        this.text = text;
        this.documentFrequency = documentFrequency;
        this.postingPosition = postingPosition;
        this.stats = new TermStatistics(documentFrequency, 0, postingPosition);
    }

    public String getField() {
        return field;
    }

    public String getText() {
        return text;
    }

    public int getDocumentFrequency() {
        return documentFrequency;
    }

    public long getPostingPosition() {
        return postingPosition;
    }

    @Override
    public int compareTo(Term other) {
        int cmp = field.compareTo(other.field);

        if (cmp == 0) {
            cmp = text.compareTo(other.text);
        }

        return cmp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Term)) return false;

        Term term = (Term) o;

        return field.equals(term.field) && text.equals(term.text);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, text);
    }

    @Override
    public String toString() {
        return String.format("Term{field='%s', text='%s', df=%d, position=%d}",
                field,
                text,
                documentFrequency,
                postingPosition
        );
    }

    public static class TermStatistics {

        private int docFreq;
        private long totalTermFreq;
        private long postingOffset;
        private int lastDocId;


        public TermStatistics(int docFreq, long totalTermFreq, long postingOffset) {
            this.docFreq = docFreq;
            this.totalTermFreq = totalTermFreq;
            this.postingOffset = postingOffset;
        }

        public void updateStats(int docId, int freq) {
            docFreq++;
            totalTermFreq += freq;
            lastDocId = docId;
        }
    }
}