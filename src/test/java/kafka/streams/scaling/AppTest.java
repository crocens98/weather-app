package kafka.streams.scaling;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import kafka.streams.scaling.entity.Hotel;
import kafka.streams.scaling.entity.TemperatureRecord;
import kafka.streams.scaling.entity.Weather;
import kafka.streams.scaling.serialaization.utill.SerdesUtill;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AppTest {

  private TopologyTestDriver td;
  private final Properties config;

  private TestInputTopic<String, Hotel> hotelsInputTopic;
  private TestInputTopic<String, Weather> weatherInputTopic;
  private TestOutputTopic<String, Hotel> outputTopic;

  private Weather firstWeather;
  private Weather secondWeather;
  private Weather thirdWeather;
  private Weather fourthWeather;
  private Weather fifthWeather;

  private Hotel firstHotel;
  private Hotel secondHotel;


  public AppTest() {
    config = new Properties();
    config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, App.APP_ID);
    config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, App.BOOTSTRAP_SERVER);
  }

  @AfterEach
  public void tearDown() {
    td.close();

  }

  @BeforeEach
  public void beforeEach() {
    initData();
  }

  @Test
  void main() {
    Topology topology = App.buildTopology();
    td = new TopologyTestDriver(topology, config);
    hotelsInputTopic = td.createInputTopic(App.HOTELS_TOPIC, Serdes.String().serializer(),
        SerdesUtill.hotelJsonSerializer);
    weatherInputTopic = td.createInputTopic(App.WEATHER_TOPIC, Serdes.String().serializer(),
        SerdesUtill.weatherJsonSerializer);
    outputTopic = td.createOutputTopic(App.RESULT_TOPIC, Serdes.String().deserializer(),
        SerdesUtill.hotelJsonDeserializer);
    Assertions.assertTrue(outputTopic.isEmpty());

    weatherInputTopic.pipeInput(firstWeather);
    weatherInputTopic.pipeInput(secondWeather);
    weatherInputTopic.pipeInput(thirdWeather);
    weatherInputTopic.pipeInput(fourthWeather);
    weatherInputTopic.pipeInput(fifthWeather);

    hotelsInputTopic.pipeInput(firstHotel);
    hotelsInputTopic.pipeInput(secondHotel);

    Assertions.assertEquals(2, outputTopic.getQueueSize());
    List<KeyValue<String, Hotel>> keyValues = outputTopic.readKeyValuesToList();
    Optional<KeyValue<String, Hotel>> u176 = keyValues.stream()
        .filter(stringHotelKeyValue -> stringHotelKeyValue.key.equals("u176")).findFirst();
    Assertions.assertTrue(u176.isPresent());
    List<TemperatureRecord> u176TemperatureRecords = u176.get().value.getTemperatureRecords();
    Assertions.assertEquals(2, u176TemperatureRecords.size());

    Optional<TemperatureRecord> firstU176TemperatureRecord = u176TemperatureRecords.stream()
        .filter(temperatureRecord -> temperatureRecord.getDay().equals("2016-10-23")).findFirst();
    Assertions.assertTrue(firstU176TemperatureRecord.isPresent());
    Assertions.assertEquals((20.9 + 21.9) / 2, firstU176TemperatureRecord.get().getTemperature());

    Optional<TemperatureRecord> secondU176TemperatureRecord = u176TemperatureRecords.stream()
        .filter(temperatureRecord -> temperatureRecord.getDay().equals("2016-10-24")).findFirst();
    Assertions.assertTrue(secondU176TemperatureRecord.isPresent());
    Assertions.assertEquals(59.9, secondU176TemperatureRecord.get().getTemperature());

    Optional<KeyValue<String, Hotel>> u09w = keyValues.stream()
        .filter(stringHotelKeyValue -> stringHotelKeyValue.key.equals("u09w")).findFirst();
    Assertions.assertTrue(u09w.isPresent());
    List<TemperatureRecord> u09wTemperatureRecords = u09w.get().value.getTemperatureRecords();
    Assertions.assertEquals(1, u09wTemperatureRecords.size());

    Optional<TemperatureRecord> firstU09wTemperatureRecord = u09wTemperatureRecords.stream()
        .filter(temperatureRecord -> temperatureRecord.getDay().equals("2016-10-23")).findFirst();
    Assertions.assertTrue(firstU09wTemperatureRecord.isPresent());
    Assertions.assertEquals((20.4 + 24.4) / 2, firstU09wTemperatureRecord.get().getTemperature());
  }

  private void initData() {
    firstWeather = Weather.builder()
        .lng(24.3409)
        .lat(2.96422)
        .avgTmprF(76.8)
        .avgTmprC(20.9)
        .wthrDate("2016-10-23")
        .geohash("u176")
        .build();

    secondWeather = Weather.builder()
        .lng(24.3409)
        .lat(2.96422)
        .avgTmprF(76.8)
        .avgTmprC(21.9)
        .wthrDate("2016-10-23")
        .geohash("u176")
        .build();

    thirdWeather = Weather.builder()
        .lng(24.3409)
        .lat(2.96422)
        .avgTmprF(76.8)
        .avgTmprC(59.9)
        .wthrDate("2016-10-24")
        .geohash("u176")
        .build();

    fourthWeather = Weather.builder()
        .lng(34.159)
        .lat(2.96422)
        .avgTmprF(76.0)
        .avgTmprC(24.4)
        .wthrDate("2016-10-23")
        .geohash("u09w")
        .build();

    fifthWeather = Weather.builder()
        .lng(34.159)
        .lat(2.96422)
        .avgTmprF(76.0)
        .avgTmprC(20.4)
        .wthrDate("2016-10-23")
        .geohash("u09w")
        .build();

    firstHotel = Hotel.builder()
        .id(3418793967624L)
        .name("Room Mate Aitana")
        .country("NL")
        .city("Amsterdam")
        .address("IJdock 6 Amsterdam City Center 1013 MM Amsterdam Netherlands")
        .latitude(52.3846059)
        .longitude(4.8941866)
        .geohash("u176")
        .build();

    secondHotel = Hotel.builder()
        .id(3427383902209L)
        .name("H tel Barri re Le Fouquet s")
        .country("FR")
        .city("Paris")
        .address("46 Avenue George V 8th arr 75008 Paris France")
        .latitude(48.8710709)
        .longitude(2.3013119)
        .geohash("u09w")
        .build();
  }
}
