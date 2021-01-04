package kafka.streams.scaling.entity;

import lombok.Data;

@Data
public class TemperatureRecord {

  String day;
  double temperature;
  String geohash;
}
