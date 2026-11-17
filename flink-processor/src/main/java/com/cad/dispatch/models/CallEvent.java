package com.cad.dispatch.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CallEvent {
    @JsonProperty("call_id")
    private String callId;
    @JsonProperty("caller_timestamp")
    private String callerTimestamp;
    @JsonProperty("idempotency_key")
    private String idempotencyKey;
    @JsonProperty("incident_type_raw")
    private String incidentTypeRaw;
    @JsonProperty("sector_id")
    private String sectorId;
    @JsonProperty("dispatcher_notes")
    private String dispatcherNotes;

    // Default constructor required for deserialization
    public CallEvent() {}

    public CallEvent(String callId, String callerTimestamp, String idempotencyKey,
                     String incidentTypeRaw, String sectorId, String dispatcherNotes) {
        this.callId = callId;
        this.callerTimestamp = callerTimestamp;
        this.idempotencyKey = idempotencyKey;
        this.incidentTypeRaw = incidentTypeRaw;
        this.sectorId = sectorId;
        this.dispatcherNotes = dispatcherNotes;
    }

    public String getCallId() { return callId; }
    public String getCallerTimestamp() { return callerTimestamp; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public String getIncidentTypeRaw() { return incidentTypeRaw; }
    public String getSectorId() { return sectorId; }
    public String getDispatcherNotes() { return dispatcherNotes; }
}