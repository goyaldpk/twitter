import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class JsonSerializer implements DeserializationSchema<JsonNode>, SerializationSchema<JsonNode> {
    ObjectMapper om = new ObjectMapper();
    @Override
    public JsonNode deserialize(byte[] bytes) throws IOException {
        final ObjectReader reader = om.reader();
        final JsonNode newNode = reader.readTree(new ByteArrayInputStream(bytes));
        return newNode;
    }

    @Override
    public boolean isEndOfStream(JsonNode jsonNode) {
        return false;
    }

    @Override
    public byte[] serialize(JsonNode jsonNode) {
        final ObjectWriter writer = om.writer();
        try {
            final byte[] bytes = writer.writeValueAsBytes(jsonNode);
            return bytes;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public TypeInformation<JsonNode> getProducedType() {
        return TypeInformation.of(JsonNode.class);
    }
}
