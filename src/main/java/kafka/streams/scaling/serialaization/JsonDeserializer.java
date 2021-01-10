package kafka.streams.scaling.serialaization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import kafka.streams.scaling.util.JsonConverter;
import org.apache.kafka.common.serialization.Deserializer;


/**
 * @param <T> Entity which will be deserialized from json
 */
public class JsonDeserializer<T> implements Deserializer<T> {

  private Class<?> type;
  private final JsonConverter jsonConverter;

  /**
   * Default constructor needed by kafka
   * @param jsonConverter instance of JsonConverter uses to parse json
   */

  public JsonDeserializer(JsonConverter jsonConverter) {
    this.jsonConverter = jsonConverter;
  }

  public JsonDeserializer(Class<?> type, JsonConverter jsonConverter) {
    this.type = type;
    this.jsonConverter = jsonConverter;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> map, boolean arg1) {
    if (type == null) {
      type = (Class<T>) map.get("type");
    }

  }

  @Override
  public T deserialize(String undefined, byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return null;
    }
    Object object = jsonConverter.toObject(new String(bytes), type, this);
    if (object.getClass().isArray()) {
      return (T) new ArrayList<>(Arrays.asList(object));
    } else {
      return (T) object;
    }
  }

  protected Class<?> getType() {
    return type;
  }

}
