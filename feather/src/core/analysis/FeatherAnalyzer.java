package core.analysis;

import java.util.List;

public interface FeatherAnalyzer {
    List<FeatherToken> analyze(String text);
}