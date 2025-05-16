package com.q1.model;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import com.q1.GameDataProcessor.*;

public class PaymentEventDeserializationSchema implements DeserializationSchema<PaymentEvent> {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public PaymentEvent deserialize(byte[] message) throws IOException {
        return mapper.readValue(message, PaymentEvent.class);
    }

    @Override
    public boolean isEndOfStream(PaymentEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<PaymentEvent> getProducedType() {
        return TypeInformation.of(PaymentEvent.class);
    }
}

// PaymentEventDeserializationSchema实现类似