package org.myorg.quickstart.cep.model;

import java.io.Serializable;
import java.util.Map;

public class RowData implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<String, Object> fields;

    public RowData(Map<String, Object> fields) {
        this.fields = fields;
    }

    public Map<String, Object> getFields() {
        return fields;
    }
}
