package kafka.streams.scaling.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder(toBuilder=true)
public class WeatherGroupingKey {

  private String wthrDate;
  private String geohash;
}
