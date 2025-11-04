package core.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.stream.Stream;

public class LuceneAnalyzerAdapter implements FeatherAnalyzer {

    private final Analyzer luceneAnalyzer;

    public LuceneAnalyzerAdapter(Analyzer luceneAnalyzer) {
        this.luceneAnalyzer = luceneAnalyzer;
    }

    @Override
    public Stream<FeatherToken> analyze(String text) {
        try {
            TokenStream tokenStream = luceneAnalyzer.tokenStream(null, text);
            tokenStream.reset();

            CharTermAttribute termAttr = tokenStream.getAttribute(CharTermAttribute.class);
            OffsetAttribute offsetAttr = tokenStream.getAttribute(OffsetAttribute.class);

            return Stream.generate(() -> {
                        try {
                            if (tokenStream.incrementToken()) {
                                return new FeatherToken(termAttr.toString(), offsetAttr.startOffset(), offsetAttr.endOffset());
                            }
                            return null;
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    })
                    .takeWhile(Objects::nonNull)
                    .onClose(() -> {
                        try {
                            tokenStream.end();
                            tokenStream.close();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create TokenStream from analyzer", e);
        }
    }
}
