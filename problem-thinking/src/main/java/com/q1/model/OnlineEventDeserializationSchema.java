package com.q1.model;

import com.q1.GameDataProcessor.*;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class OnlineEventDeserializationSchema implements DeserializationSchema<OnlineEvent> {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public OnlineEvent deserialize(byte[] message) throws IOException {
        return mapper.readValue(message, OnlineEvent.class);
    }

    @Override
    public boolean isEndOfStream(OnlineEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<OnlineEvent> getProducedType() {
        return TypeInformation.of(OnlineEvent.class);
    }
}

// PaymentEventDeserializationSchema实现类似