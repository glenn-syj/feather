import core.analysis.FeatherAnalyzer;
import core.analysis.FeatherToken;
import core.analysis.LuceneAnalyzerAdapter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class LuceneAnalyzerAdapterTest {

    private FeatherAnalyzer featherAnalyzer;

    @BeforeEach
    // StandardAnalyzer does not remove common stop words!
    void setUp() {
        featherAnalyzer = new LuceneAnalyzerAdapter(new StandardAnalyzer());
    }

    @Test
    void analyze_SimpleText_ReturnsCorrectTokens() {
        // Given
        String text = "Hello World";

        // When
        List<FeatherToken> tokens = featherAnalyzer.analyze(text).collect(Collectors.toList());

        // Then
        assertNotNull(tokens);
        assertEquals(2, tokens.size());
        assertEquals(new FeatherToken("hello", 0, 5), tokens.get(0));
        assertEquals(new FeatherToken("world", 6, 11), tokens.get(1));
    }

    @Test
    void analyze_TextWithMultipleTokens_ReturnsCorrectTokens() {
        // Given
        String text = "This is a test sentence.";

        // When
        List<FeatherToken> tokens = featherAnalyzer.analyze(text).collect(Collectors.toList());

        // Then
        assertNotNull(tokens);
        assertEquals(5, tokens.size());
        assertEquals(new FeatherToken("this", 0, 4), tokens.get(0));
        assertEquals(new FeatherToken("is", 5, 7), tokens.get(1));
        assertEquals(new FeatherToken("sentence", 15, 23), tokens.get(4));
    }

    @Test
    void analyze_EmptyString_ReturnsEmptyList() {
        // Given
        String text = "";

        // When
        List<FeatherToken> tokens = featherAnalyzer.analyze(text).collect(Collectors.toList());

        // Then
        assertNotNull(tokens);
        assertTrue(tokens.isEmpty());
    }

    @Test
    void analyze_TextWithSpecialCharacters_HandlesCorrectly() {
        // Given
        // characters like `-`, `.`, `( )` get removed on StandardAnalyzer
        String text = "apple-pie, banana. (cherry)";

        // When
        List<FeatherToken> tokens = featherAnalyzer.analyze(text).collect(Collectors.toList());
        System.out.println(Arrays.toString(tokens.toArray()));

        // Then
        assertNotNull(tokens);

        // apple, pie, banana, cherry
        assertEquals(4, tokens.size());
        assertEquals(new FeatherToken("apple", 0, 5), tokens.get(0));
        assertEquals(new FeatherToken("banana", 11, 17), tokens.get(2));
        assertEquals(new FeatherToken("cherry", 20, 26), tokens.get(3));
    }

    @Test
    void analyze_OffsetsAreCorrect() {
        // Given
        String text = "  leading and trailing spaces  ";

        // When
        List<FeatherToken> tokens = featherAnalyzer.analyze(text).collect(Collectors.toList());

        // Then
        assertNotNull(tokens);
        assertEquals(4, tokens.size());
        assertEquals(new FeatherToken("leading", 2, 9), tokens.get(0));
        assertEquals(new FeatherToken("and", 10, 13), tokens.get(1));
        assertEquals(new FeatherToken("trailing", 14, 22), tokens.get(2));
        assertEquals(new FeatherToken("spaces", 23, 29), tokens.get(3));
    }

    @Test
    void analyze_KoreanText_ReturnsCorrectTokens() {
        // Given
        String text = "안녕하세요 환영합니다";

        // When
        List<FeatherToken> tokens = featherAnalyzer.analyze(text).collect(Collectors.toList());

        // Then
        assertNotNull(tokens);
        // StandardAnalyzer treats Korean characters as a single token or breaks them by whitespace
        // It might not produce linguistically correct tokens for Korean, but we test its behavior.
        assertEquals(2, tokens.size());
        assertEquals(new FeatherToken("안녕하세요", 0, 5), tokens.get(0));
        assertEquals(new FeatherToken("환영합니다", 6, 11), tokens.get(1));
    }
}