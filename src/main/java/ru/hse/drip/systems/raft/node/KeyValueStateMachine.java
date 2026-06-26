package ru.hse.drip.systems.raft.node;

import java.util.HashMap;
import java.util.Map;

public class KeyValueStateMachine {
    private final Map<String, String> values = new HashMap<>();

    public void apply(String command) {
        if (command == null) {
            return;
        }
        String[] parts = command.split("\\s+", 3);
        if (parts.length == 3 && "put".equalsIgnoreCase(parts[0])) {
            values.put(parts[1], parts[2]);
        }
    }

    public String get(String key) {
        return values.get(key);
    }

    public void clear() {
        values.clear();
    }
}
