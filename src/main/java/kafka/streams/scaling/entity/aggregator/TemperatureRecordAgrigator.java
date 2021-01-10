package kafka.streams.scaling.entity.aggregator;

import java.util.Set;
import kafka.streams.scaling.entity.TemperatureRecord;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TemperatureRecordAgrigator {

  private Set<TemperatureRecord> temperatureRecords;
}
