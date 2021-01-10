package kafka.streams.scaling.serialaization;

import java.util.Map;
import kafka.streams.scaling.util.JsonConverter;
import org.apache.kafka.common.serialization.Serializer;

/**
 * @param <T> Entity which will be serialized to json
 */
public class JsonSerializer<T> implements Serializer<T> {

  private final JsonConverter jsonConverter;

  public JsonSerializer(JsonConverter jsonConverter) {
    this.jsonConverter = jsonConverter;
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {

  }

  @Override
  public byte[] serialize(String topic, T data) {
    return jsonConverter.toJson(data).getBytes();
  }

}
