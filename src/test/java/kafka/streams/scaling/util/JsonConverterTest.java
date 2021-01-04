package kafka.streams.scaling.util;

import com.google.gson.FieldNamingPolicy;
import java.util.ArrayList;
import java.util.List;
import kafka.streams.scaling.entity.TemperatureRecord;
import kafka.streams.scaling.entity.Weather;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class JsonConverterTest {

  @Test
  void toObject_hotel() {

    String testData =
        "{\"Id\":\"3427383902209\",\"Name\":\"H tel Barri re Le Fouquet s\",\"Country\":\"FR\","
            + "\"City\":\"Paris\",\"Address\":\"46 Avenue George V 8th arr 75008 Paris France\",\"Latitude\":\"48"
            + ".8710709\",\"Longitude\":\"2.3013119\",\"Geohash\":\"u09w\" ,"
            + " \"TemperatureRecords\" : [{\"Day\" : \"2017-09-04\", \"Temperature\" : \"24.123\"}, {\"Day\" : "
            + "\"2017-08-04\", \"Temperature\" : \"28.123\"}]}";

    TemperatureRecord temperatureRecord1 = new TemperatureRecord();
    temperatureRecord1.setDay("2017-09-04");
    temperatureRecord1.setTemperature(24.123);

    TemperatureRecord temperatureRecord2 = new TemperatureRecord();
    temperatureRecord2.setDay("2017-08-04");
    temperatureRecord2.setTemperature(28.123);

    List<TemperatureRecord> set = new ArrayList<>();
    set.add(temperatureRecord1);
    set.add(temperatureRecord2);

    Hotel expected = new Hotel();
    expected.setId(3427383902209L);
    expected.setName("H tel Barri re Le Fouquet s");
    expected.setCountry("FR");
    expected.setCity("Paris");
    expected.setAddress("46 Avenue George V 8th arr 75008 Paris France");
    expected.setLatitude(48.8710709);
    expected.setLongitude(2.3013119);
    expected.setGeohash("u09w");
    expected.setTemperatureRecords(set);

    JsonConverter jsonConverter = new JsonConverter(FieldNamingPolicy.UPPER_CAMEL_CASE);
    Hotel hotel = jsonConverter.toObject(testData, Hotel.class, null);

    Assertions.assertEquals(expected, hotel);
  }


  @Test
  void toObject_weather() {

    String testData = "{\"lng\":35.3863,\"lat\":-24.0204,\"avg_tmpr_f\":73.3,\"avg_tmpr_c\":22.9,"
        + "\"wthr_date\":\"2016-10-19\",\"geohash\":\"kg9p\"}";

    String testData2 = "[{\"lng\":35.3863,\"lat\":-24.0204,\"avg_tmpr_f\":73.3,\"avg_tmpr_c\":22.9,"
        + "\"wthr_date\":\"2016-10-19\",\"geohash\":\"kg9p\"}]";
    Weather expected = new Weather();
    expected.setLng(35.3863);
    expected.setLat(-24.0204);
    expected.setAvgTmprF(73.3);
    expected.setAvgTmprC(22.9);
    expected.setWthrDate("2016-10-19");
    expected.setGeohash("kg9p");

    JsonConverter jsonConverter = new JsonConverter(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES);
    Weather weather = jsonConverter.toObject(testData, Weather.class, null);

    Object weathers = jsonConverter.toObject(testData2, Weather[].class, null);
    Assertions.assertEquals(expected, weather);
  }

}
