package com.azure.cosmos.bench;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * The synthetic "fake mode" document produced by the benchmark, matching the shape used by the
 * Python and .NET implementations:
 *
 * <pre>
 *   id     : string (uuid)
 *   docid  : string (uuid)   -- partition key /docid
 *   title  : string
 *   text   : string          -- ~PAYLOAD_BYTES filler
 *   emb    : array&lt;float&gt;    -- FAKE_DATA_VECTOR_DIM floats in [-1, 1]
 * </pre>
 */
public final class VectorDoc {
    @JsonProperty("id")
    public String id;
    @JsonProperty("docid")
    public String docid;
    @JsonProperty("title")
    public String title;
    @JsonProperty("text")
    public String text;
    @JsonProperty("emb")
    public List<Float> emb;

    public VectorDoc() {
    }

    public VectorDoc(String id, String docid, String title, String text, List<Float> emb) {
        this.id = id;
        this.docid = docid;
        this.title = title;
        this.text = text;
        this.emb = emb;
    }
}
