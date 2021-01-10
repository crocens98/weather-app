package kafka.streams.scaling.entity;

import java.util.ArrayList;
import java.util.List;
import lombok.Builder;
import lombok.Data;

@Data
@Builder(toBuilder=true)
public class Hotel {

  private long id;
  private String name;
  private String country;
  private String city;
  private String address;
  private double latitude;
  private double longitude;
  private String geohash;

  private List<TemperatureRecord> temperatureRecords = new ArrayList<>();
}
