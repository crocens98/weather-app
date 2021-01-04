package kafka.streams.scaling;

import com.google.gson.FieldNamingPolicy;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import kafka.streams.scaling.entity.Weather;
import kafka.streams.scaling.joiner.TemperatureRecordsHotelJoiner;
import kafka.streams.scaling.serialaization.JsonDeserializer;
import kafka.streams.scaling.serialaization.JsonSerializer;
import kafka.streams.scaling.util.JsonConverter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.log4j.Logger;

public class App2 {

  private static final Logger log = Logger.getLogger(App.class);

  private static final String weatherTopic = "weather_data";

  private static final TemperatureRecordsHotelJoiner joiner = new TemperatureRecordsHotelJoiner();

  private static Serde<Weather> weatherSerde;
  private static final JsonConverter hotelJsonConverter = new JsonConverter(
      FieldNamingPolicy.UPPER_CAMEL_CASE);
  private static final JsonConverter weatherJsonConverter = new JsonConverter(
      FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES);

  public static void main(String[] args) {

    initSerde();

    StreamsBuilder builder = new StreamsBuilder();

    builder.stream(weatherTopic,
        Consumed.with(Serdes.String(), weatherSerde)).selectKey((key, value) -> "")
        .groupByKey(Grouped.with(Serdes.String(), weatherSerde)).count().suppress(
        Suppressed.untilTimeLimit(Duration.ofSeconds(5), Suppressed.BufferConfig.unbounded()))
        .toStream()
        .foreach((key, value) -> {
          System.out.println("RESULT " + value);
        });

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


  private static void initSerde() {
    JsonSerializer<Weather> weatherJsonSerializer = new JsonSerializer<>(weatherJsonConverter);
    JsonDeserializer<Weather> weatherJsonDeserializer = new JsonDeserializer<>(Weather.class,
        weatherJsonConverter);
    weatherSerde = Serdes.serdeFrom(weatherJsonSerializer, weatherJsonDeserializer);


  }

  private static Properties provideProperties() {
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG,
        "weather-app-v4");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
        Optional.ofNullable(System.getenv("BOOTSTRAP_SERVERS_CONFIG")).orElse("localhost:9092"));
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
    return config;
  }
}
