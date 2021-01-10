package kafka.streams.scaling;

import java.util.Comparator;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import kafka.streams.scaling.entity.Hotel;
import kafka.streams.scaling.entity.TemperatureRecord;
import kafka.streams.scaling.entity.Weather;
import kafka.streams.scaling.entity.WeatherGroupingKey;
import kafka.streams.scaling.entity.aggregator.CountAndSum;
import kafka.streams.scaling.entity.aggregator.TemperatureRecordAgrigator;
import kafka.streams.scaling.joiner.TemperatureRecordsHotelJoiner;
import kafka.streams.scaling.serialaization.utill.SerdesUtill;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.log4j.Logger;

public class App {

  private static final Logger log = Logger.getLogger(App.class);

  public static final String HOTELS_TOPIC = System.getProperty("hotels_data", "hotels_data");
  public static final String WEATHER_TOPIC = System.getProperty("weather_data", "weather_data");
  public static final String APP_ID = System.getProperty("weather-app", "weather-app-v10");
  public static final String RESULT_TOPIC = System.getProperty("result-topic", "result-topic");
  public static final String BOOTSTRAP_SERVER = System
      .getProperty("BOOTSTRAP_SERVERS_CONFIG", "localhost:9092");

  private static final TemperatureRecordsHotelJoiner joiner = new TemperatureRecordsHotelJoiner();

  private static long counter1;
  private static long counter2;

  public static void main(String[] args) {
    Topology topology = buildTopology();
    KafkaStreams streams = new KafkaStreams(topology, provideProperties());
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

  private static Properties provideProperties() {
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG,
        APP_ID);
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000 * 60);
    config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
    return config;
  }

  public static Topology buildTopology() {
    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, Hotel> hotelsStream = builder
        .stream(HOTELS_TOPIC, Consumed.with(Serdes.String(), SerdesUtill.hotelSerde));
    KStream<String, Weather> weatherStream = builder.stream(WEATHER_TOPIC,
        Consumed.with(Serdes.String(), SerdesUtill.weatherSerde));

    KTable<String, TemperatureRecordAgrigator> temperatureRecordsTable = buildTemperatureRecordsTable(
        weatherStream);
    KStream<String, Hotel> hotelsTStream = buildHotelsTable(hotelsStream);

    hotelsTStream.leftJoin(temperatureRecordsTable, joiner,
        Joined.with(Serdes.String(), SerdesUtill.hotelSerde, SerdesUtill.temperatureRecordsSerde))
        .to(RESULT_TOPIC, Produced.with(Serdes.String(), SerdesUtill.hotelSerde));
    return builder.build();
  }

  private static CountAndSum countAndSumAggregation(CountAndSum aggregate, Weather value) {
    if (counter1 % 1_000_000 == 0) {
      log.info("SUM AGGREGATION " + counter1);
    }
    counter1++;
    aggregate.setCount(aggregate.getCount() + 1);
    aggregate.setSum(aggregate.getSum() + value.getAvgTmprC());
    return aggregate;
  }

  private static KeyValue<String, TemperatureRecord> initKeyValueRecord(WeatherGroupingKey key,
      Double value) {
    TemperatureRecord temperatureRecord = TemperatureRecord
        .builder()
        .day(key.getWthrDate())
        .temperature(value)
        .geohash(key.getGeohash())
        .build();
    return KeyValue.pair(key.getGeohash(), temperatureRecord);
  }

  private static KTable<String, TemperatureRecordAgrigator> buildTemperatureRecordsTable(
      KStream<String, Weather> weatherStream) {
    return weatherStream.filter((key, value) -> value.getGeohash() != null)
        .selectKey(
            (k, weather) -> WeatherGroupingKey.builder().wthrDate(weather.getWthrDate())
                .geohash(weather.getGeohash()).build()
        )
        .groupByKey(
            Grouped.with(SerdesUtill.weatherGroupingKeySerde, SerdesUtill.weatherSerde))
        .aggregate(() -> new CountAndSum(0L, 0.0),
            (key, value, aggregate) -> countAndSumAggregation(aggregate, value),
            Materialized.with(SerdesUtill.weatherGroupingKeySerde, SerdesUtill.countAndSumSerde))
        .mapValues(
            value -> value.getSum() / value.getCount(),
            Materialized.with(SerdesUtill.weatherGroupingKeySerde, Serdes.Double()))
        .toStream()
        .map(App::initKeyValueRecord)
        .groupByKey(Grouped.with(Serdes.String(), SerdesUtill.temperatureRecordSerde))
        .aggregate(
            () -> TemperatureRecordAgrigator.builder()
                .temperatureRecords(new TreeSet<>(Comparator.comparing(TemperatureRecord::getDay)))
                .build()
            , (key, value, aggregate) -> {
              if (counter2 % 1_000_000 == 0) {
                log.info("TEMPERATURE RECORDS AGGREGATION " + counter2);
              }
              counter2++;

              Set<TemperatureRecord> temperatureRecords = new TreeSet<>(
                  Comparator.comparing(TemperatureRecord::getDay));
              temperatureRecords.addAll(aggregate.getTemperatureRecords());
              temperatureRecords.remove(value);
              temperatureRecords.add(value);
              aggregate.setTemperatureRecords(temperatureRecords);
              return aggregate;
            }, Materialized.with(Serdes.String(), SerdesUtill.temperatureRecordsSerde));
  }

  private static KStream<String, Hotel> buildHotelsTable(KStream<String, Hotel> hotelsStream) {
    return hotelsStream.selectKey((key, hotel) -> hotel.getGeohash());
  }
}

