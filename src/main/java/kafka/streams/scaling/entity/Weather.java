package kafka.streams.scaling.entity;

import lombok.Data;

@Data
public class Weather {

  private double lng;
  private double lat;
  private double avgTmprF;
  private double avgTmprC;
  private String wthrDate;
  private String geohash;
}
