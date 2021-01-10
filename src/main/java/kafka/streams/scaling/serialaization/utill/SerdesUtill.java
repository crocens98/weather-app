package kafka.streams.scaling.serialaization.utill;

import com.google.gson.FieldNamingPolicy;
import kafka.streams.scaling.entity.Hotel;
import kafka.streams.scaling.entity.TemperatureRecord;
import kafka.streams.scaling.entity.Weather;
import kafka.streams.scaling.entity.WeatherGroupingKey;
import kafka.streams.scaling.entity.aggregator.CountAndSum;
import kafka.streams.scaling.entity.aggregator.TemperatureRecordAgrigator;
import kafka.streams.scaling.serialaization.JsonDeserializer;
import kafka.streams.scaling.serialaization.JsonSerializer;
import kafka.streams.scaling.util.JsonConverter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

/**
 * Class provide utils serdes to use it in app and unit tests
 */
public class SerdesUtill {

  public static final Serde<TemperatureRecord> temperatureRecordSerde;
  public static final Serde<Hotel> hotelSerde;
  public static final Serde<Weather> weatherSerde;
  public static final Serde<CountAndSum> countAndSumSerde;
  public static final Serde<TemperatureRecordAgrigator> temperatureRecordsSerde;
  public static final Serde<WeatherGroupingKey> weatherGroupingKeySerde;

  public static final JsonSerializer<Hotel> hotelJsonSerializer;
  public static final JsonDeserializer<Hotel> hotelJsonDeserializer;
  public static final JsonSerializer<Weather> weatherJsonSerializer;
  public static final JsonDeserializer<Weather> weatherJsonDeserializer;
  public static final JsonSerializer<WeatherGroupingKey> weatherGroupingKeyJsonSerializer;
  public static final JsonDeserializer<WeatherGroupingKey> weatherGroupingKeyJsonDeserializer;
  public static final JsonSerializer<CountAndSum> countAndSumSerializer;
  public static final JsonDeserializer<CountAndSum> countAndSumDeserializer;
  public static final JsonSerializer<TemperatureRecordAgrigator> temperatureRecordsJsonSerializer;
  public static final JsonDeserializer<TemperatureRecordAgrigator> temperatureRecordsJsonDeserializer;
  public static final JsonSerializer<TemperatureRecord> temperatureRecordJsonSerializer;
  public static final JsonDeserializer<TemperatureRecord> temperatureRecordJsonDeserializer;

  static {
    JsonConverter hotelJsonConverter = new JsonConverter(
        FieldNamingPolicy.UPPER_CAMEL_CASE);

    JsonConverter weatherJsonConverter = new JsonConverter(
        FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES);

    hotelJsonSerializer = new JsonSerializer<>(hotelJsonConverter);
    hotelJsonDeserializer = new JsonDeserializer<>(Hotel.class,
        hotelJsonConverter);

    weatherJsonSerializer = new JsonSerializer<>(weatherJsonConverter);
    weatherJsonDeserializer = new JsonDeserializer<>(Weather.class,
        weatherJsonConverter);

    weatherGroupingKeyJsonSerializer = new JsonSerializer<>(
        weatherJsonConverter);
    weatherGroupingKeyJsonDeserializer = new JsonDeserializer<>(
        WeatherGroupingKey.class, weatherJsonConverter);

    countAndSumSerializer = new JsonSerializer<>(weatherJsonConverter);
    countAndSumDeserializer = new JsonDeserializer<>(
        CountAndSum.class,
        weatherJsonConverter);

    temperatureRecordsJsonSerializer = new JsonSerializer<>(
        weatherJsonConverter);
    temperatureRecordsJsonDeserializer = new JsonDeserializer<>(
        TemperatureRecordAgrigator.class, weatherJsonConverter);

    temperatureRecordJsonSerializer = new JsonSerializer<>(
        weatherJsonConverter);
    temperatureRecordJsonDeserializer = new JsonDeserializer<>(
        TemperatureRecord.class, weatherJsonConverter);

    hotelSerde = Serdes.serdeFrom(hotelJsonSerializer, hotelJsonDeserializer);

    weatherSerde = Serdes.serdeFrom(weatherJsonSerializer, weatherJsonDeserializer);

    weatherGroupingKeySerde = Serdes.serdeFrom(weatherGroupingKeyJsonSerializer,
        weatherGroupingKeyJsonDeserializer);

    countAndSumSerde = Serdes.serdeFrom(countAndSumSerializer, countAndSumDeserializer);

    temperatureRecordsSerde = Serdes.serdeFrom(temperatureRecordsJsonSerializer,
        temperatureRecordsJsonDeserializer);

    temperatureRecordSerde = Serdes
        .serdeFrom(temperatureRecordJsonSerializer, temperatureRecordJsonDeserializer);
  }

}
