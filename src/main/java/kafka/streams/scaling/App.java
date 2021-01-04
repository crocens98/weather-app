package kafka.streams.scaling;

import com.google.gson.FieldNamingPolicy;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import kafka.streams.scaling.entity.Hotel;
import kafka.streams.scaling.entity.TemperatureRecord;
import kafka.streams.scaling.entity.Weather;
import kafka.streams.scaling.entity.WeatherGroupingKey;
import kafka.streams.scaling.entity.aggregator.CountAndSum;
import kafka.streams.scaling.entity.aggregator.TemperatureRecordAgrigator;
import kafka.streams.scaling.joiner.TemperatureRecordsHotelJoiner;
import kafka.streams.scaling.serialaization.JsonDeserializer;
import kafka.streams.scaling.serialaization.JsonSerializer;
import kafka.streams.scaling.util.JsonConverter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.log4j.Logger;

public class App {

  private static final Logger log = Logger.getLogger(App.class);

  private static final String hotelsTopic = "test";
  private static final String weatherTopic = "weather_data";

  private static final TemperatureRecordsHotelJoiner joiner = new TemperatureRecordsHotelJoiner();

  private static Serde<TemperatureRecord> temperatureRecordSerde;
  private static Serde<Hotel> hotelSerde;
  private static Serde<Weather> weatherSerde;
  private static Serde<WeatherGroupingKey> weatherGroupingKeySerde;
  private static Serde<CountAndSum> countAndSumSerde;
  private static Serde<TemperatureRecordAgrigator> temperatureRecordsSerde;

  private static final JsonConverter hotelJsonConverter = new JsonConverter(
      FieldNamingPolicy.UPPER_CAMEL_CASE);
  private static final JsonConverter weatherJsonConverter = new JsonConverter(
      FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES);

  public static void main(String[] args) {

    initSerde();

    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, Hotel> hotelsStream = builder
        .stream(hotelsTopic, Consumed.with(Serdes.String(), hotelSerde));
    KStream<String, Weather> weatherStream = builder.stream(weatherTopic,
        Consumed.with(Serdes.String(), weatherSerde));

    KTable<String, TemperatureRecordAgrigator> temperatureRecordsTable = buildTemperatureRecordsTable(
        weatherStream);
    KStream<String, Hotel> hotelsTStream = buildHotelsTable(hotelsStream);

    hotelsTStream.leftJoin(temperatureRecordsTable, joiner, Joined.with(Serdes.String(), hotelSerde, temperatureRecordsSerde ))
        .to("result-topic", Produced.with(Serdes.String(), hotelSerde));

    KafkaStreams streams = new KafkaStreams(builder.build(), provideProperties());
    streams.cleanUp();
    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        streams.close();
        log.info("Stream stopped");
      } catch (Exception exc) {
        log.error("Got exception while executing shutdown hook: ", exc);
      }
    }));
  }

  private static CountAndSum countAndSumAggregation(CountAndSum aggregate, Weather value) {
    aggregate.setCount(aggregate.getCount() + 1);
    aggregate.setSum(aggregate.getSum() + value.getAvgTmprC());
    return aggregate;
  }

  private static KeyValue<String, TemperatureRecord> initKeyValueRecord(WeatherGroupingKey key,
      Double value) {
    TemperatureRecord temperatureRecord = new TemperatureRecord();
    temperatureRecord.setDay(key.getWthrDate());
    temperatureRecord.setTemperature(value);
    temperatureRecord.setGeohash(key.getGeohash());
    return KeyValue.pair(key.getGeohash(), temperatureRecord);
  }

  private static KTable<String, TemperatureRecordAgrigator> buildTemperatureRecordsTable(
      KStream<String, Weather> weatherStream) {
    return weatherStream.filter((key, value) -> value.getGeohash() != null)
        .selectKey(
            (k, weather) -> new WeatherGroupingKey(weather.getWthrDate(), weather.getGeohash()))
        .groupByKey(
            Grouped.with(weatherGroupingKeySerde, weatherSerde))
        .aggregate(() -> new CountAndSum(0L, 0.0),
            (key, value, aggregate) -> countAndSumAggregation(aggregate, value),
            Materialized.with(weatherGroupingKeySerde, countAndSumSerde)).mapValues(
            value -> value.getSum() / value.getCount(),
            Materialized.with(weatherGroupingKeySerde, Serdes.Double()))
        .suppress(Suppressed.untilTimeLimit(Duration.ofSeconds(5), Suppressed.BufferConfig.unbounded()))
        .toStream()
        .map(App::initKeyValueRecord)
        .groupByKey(Grouped.with(Serdes.String(), temperatureRecordSerde))
        .aggregate(
            TemperatureRecordAgrigator::new, (key, value, aggregate) -> {
              aggregate.getTemperatureRecords().add(value);
              return aggregate;
            }, Materialized.with(Serdes.String(), temperatureRecordsSerde)).suppress(
            Suppressed.untilTimeLimit(Duration.ofSeconds(5 * 60 * 10), Suppressed.BufferConfig.unbounded()));
  }

  private static KStream<String, Hotel> buildHotelsTable(KStream<String, Hotel> hotelsStream) {
    return hotelsStream.selectKey((key, hotel) -> hotel.getGeohash());
  }

  private static void initSerde() {
    JsonSerializer<Hotel> hotelJsonSerializer = new JsonSerializer<>(hotelJsonConverter);
    JsonDeserializer<Hotel> hotelJsonDeserializer = new JsonDeserializer<>(Hotel.class,
        hotelJsonConverter);

    JsonSerializer<Weather> weatherJsonSerializer = new JsonSerializer<>(weatherJsonConverter);
    JsonDeserializer<Weather> weatherJsonDeserializer = new JsonDeserializer<>(Weather.class,
        weatherJsonConverter);

    JsonSerializer<WeatherGroupingKey> weatherGroupingKeyJsonSerializer = new JsonSerializer<>(
        weatherJsonConverter);
    JsonDeserializer<WeatherGroupingKey> weatherGroupingKeyJsonDeserializer = new JsonDeserializer<>(
        WeatherGroupingKey.class, weatherJsonConverter);

    JsonSerializer<CountAndSum> countAndSumSerializer = new JsonSerializer<>(weatherJsonConverter);
    JsonDeserializer<CountAndSum> countAndSumDeserializer = new JsonDeserializer<>(
        CountAndSum.class,
        weatherJsonConverter);

    JsonSerializer<TemperatureRecordAgrigator> temperatureRecordsJsonSerializer = new JsonSerializer<>(
        weatherJsonConverter);
    JsonDeserializer<TemperatureRecordAgrigator> temperatureRecordsJsonDeserializer = new JsonDeserializer<>(
        TemperatureRecordAgrigator.class, weatherJsonConverter);

    JsonSerializer<TemperatureRecord> temperatureRecordJsonSerializer = new JsonSerializer<>(
        weatherJsonConverter);
    JsonDeserializer<TemperatureRecord> temperatureRecordJsonDeserializer = new JsonDeserializer<>(
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

  private static Properties provideProperties() {
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG,
        "weather-app-v1");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
        Optional.ofNullable(System.getenv("BOOTSTRAP_SERVERS_CONFIG")).orElse("localhost:9092"));
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
    return config;
  }
}

