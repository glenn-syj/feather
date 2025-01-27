package storage.file;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Document {
    private final int id;
    private final Map<String, Object> fields;
    private final long timestamp;

    public Document(int id) {
        this(id, System.currentTimeMillis());
    }

    public Document(int id, long timestamp) {
        this.id = id;
        this.timestamp = timestamp;
        this.fields = new HashMap<>();
    }

    public int getId() {
        return id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void addField(String name, Object value) {
        fields.put(name, value);
    }

    public Object getField(String name) {
        return fields.get(name);
    }

    public Map<String, Object> getFields() {
        return Collections.unmodifiableMap(fields);
    }
}