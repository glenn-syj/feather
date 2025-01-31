package storage.file;

import java.util.Arrays;
import java.util.Objects;

public class Posting implements Comparable<Posting> {
    private final int documentId;
    private final int frequency;
    private final int[] positions;

    public Posting(int documentId, int frequency, int[] positions) {
        validateInputs(documentId, frequency, positions);
        this.documentId = documentId;
        this.frequency = frequency;
        this.positions = positions.clone();
        Arrays.sort(this.positions);
    }

    private void validateInputs(int documentId, int frequency, int[] positions) {
        if (documentId < 0) {
            throw new IllegalArgumentException("Document ID cannot be negative");
        }
        if (frequency < 0) {
            throw new IllegalArgumentException("Frequency cannot be negative");
        }
        if (positions == null) {
            throw new IllegalArgumentException("Positions array cannot be null");
        }
        if (positions.length != frequency) {
            throw new IllegalArgumentException(
                    "Positions array length must match frequency");
        }
        if (Arrays.stream(positions).anyMatch(p -> p < 0)) {
            throw new IllegalArgumentException("Position values cannot be negative");
        }
    }

    public int getDocumentId() { return documentId; }
    public int getFrequency() { return frequency; }
    public int[] getPositions() { return positions.clone(); }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Posting posting = (Posting) o;
        return documentId == posting.documentId &&
                frequency == posting.frequency &&
                Arrays.equals(positions, posting.positions);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(documentId, frequency);
        // reference: String class hashcode()
        result = 31 * result + Arrays.hashCode(positions);
        return result;
    }

    @Override
    public String toString() {
        return String.format("Posting{docId=%d, freq=%d, pos=%s}",
                documentId, frequency, Arrays.toString(positions));
    }

    @Override
    public int compareTo(Posting o) {
        return Integer.compare(this.documentId, o.documentId);
    }
}
