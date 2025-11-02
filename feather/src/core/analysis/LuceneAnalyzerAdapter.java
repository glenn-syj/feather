package core.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LuceneAnalyzerAdapter implements FeatherAnalyzer {

    private final Analyzer luceneAnalyzer;

    public LuceneAnalyzerAdapter(Analyzer luceneAnalyzer) {
        this.luceneAnalyzer = luceneAnalyzer;
    }

    @Override
    public List<FeatherToken> analyze(String text) {
        List<FeatherToken> tokens = new ArrayList<>();
        try (TokenStream stream = luceneAnalyzer.tokenStream(null, text)) {
            CharTermAttribute termAttr = stream.addAttribute(CharTermAttribute.class);
            OffsetAttribute offsetAttr = stream.addAttribute(OffsetAttribute.class);
            stream.reset();
            while (stream.incrementToken()) {
                tokens.add(new FeatherToken(termAttr.toString(), offsetAttr.startOffset(), offsetAttr.endOffset()));
            }
            stream.end();
        } catch (IOException e) {
            throw new RuntimeException("Lucene analysis failed", e);
        }
        return tokens;
    }
}

