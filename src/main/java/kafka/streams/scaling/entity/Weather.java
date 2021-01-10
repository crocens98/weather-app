package kafka.streams.scaling.entity;

import lombok.Builder;
import lombok.Data;

@Data
@Builder(toBuilder=true)
public class Weather {

  private double lng;
  private double lat;
  private double avgTmprF;
  private double avgTmprC;
  private String wthrDate;
  private String geohash;
}
