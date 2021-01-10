package kafka.streams.scaling.entity;

import lombok.Builder;
import lombok.Data;

@Data
@Builder(toBuilder=true)
public class TemperatureRecord {

  String day;
  double temperature;
  String geohash;
}
