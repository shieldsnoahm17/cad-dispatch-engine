package com.cad.dispatch.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record CallEvent(
        @JsonProperty("call_id") String callId,
        @JsonProperty("caller_timestamp") String callerTimestamp,
        @JsonProperty("idempotency_key") String idempotencyKey,
        @JsonProperty("incident_type_raw") String incidentTypeRaw,
        @JsonProperty("sector_id") String sectorId,
        @JsonProperty("dispatcher_notes") String dispatcherNotes
) {}