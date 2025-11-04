package core.analysis;

import java.util.stream.Stream;

public interface FeatherAnalyzer {
    Stream<FeatherToken> analyze(String text);
}