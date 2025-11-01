package core.analysis;

public class FeatherToken {
    private final String term;
    private final int startOffset;
    private final int endOffset;

    public FeatherToken(String term, int startOffset, int endOffset) {
        this.term = term;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public String term() { return term; }
    public int startOffset() { return startOffset; }
    public int endOffset() { return endOffset; }
}
