package kafka.streams.scaling.joiner;

import java.util.ArrayList;
import java.util.Collections;
import kafka.streams.scaling.entity.Hotel;
import kafka.streams.scaling.entity.aggregator.TemperatureRecordAgrigator;
import org.apache.kafka.streams.kstream.ValueJoiner;

public class TemperatureRecordsHotelJoiner implements
    ValueJoiner<Hotel, TemperatureRecordAgrigator, Hotel> {

  @Override
  public Hotel apply(Hotel hotel, TemperatureRecordAgrigator temperatureRecords) {
    if (temperatureRecords != null) {
      return hotel.toBuilder().temperatureRecords(
          new ArrayList<>(temperatureRecords.getTemperatureRecords())).build();
    }
    return hotel.toBuilder().temperatureRecords(Collections.emptyList()).build();
  }
}
