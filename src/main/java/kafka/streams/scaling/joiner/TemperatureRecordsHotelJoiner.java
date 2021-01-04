package kafka.streams.scaling.joiner;

import java.util.ArrayList;
import kafka.streams.scaling.entity.Hotel;
import kafka.streams.scaling.entity.aggregator.TemperatureRecordAgrigator;
import org.apache.kafka.streams.kstream.ValueJoiner;

public class TemperatureRecordsHotelJoiner implements
    ValueJoiner<Hotel, TemperatureRecordAgrigator, Hotel> {

  @Override
  public Hotel apply(Hotel hotel, TemperatureRecordAgrigator temperatureRecords) {
    if (temperatureRecords != null) {
      hotel.setTemperatureRecords(new ArrayList<>(temperatureRecords.getTemperatureRecords()));
    }
    return hotel;
  }
}
