package kafka.streams.scaling.util;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;

public class JsonConverter {

  private final Gson gson;

  public JsonConverter(FieldNamingPolicy policy) {
    gson = new GsonBuilder().setFieldNamingPolicy(policy).setPrettyPrinting().create();
  }
  // UPPER_CAMEL_CASE Hotels
  // LOWER_CASE_WITH_UNDERSCORES weather

  public <T> T toObject(String json, Class<T> tClass, Deserializer deserializer) {
    try {
      return gson.fromJson(json, tClass);
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  public <T> String toJson(T object) {
    return gson.toJson(object);

  }
}
