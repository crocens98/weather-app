package kafka.streams.scaling.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class WeatherGroupingKey {

  private String wthrDate;
  private String geohash;
}
