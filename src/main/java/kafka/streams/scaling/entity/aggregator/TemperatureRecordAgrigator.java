package kafka.streams.scaling.entity.aggregator;

import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;
import kafka.streams.scaling.entity.TemperatureRecord;
import lombok.Data;

@Data
public class TemperatureRecordAgrigator {

  private Set<TemperatureRecord> temperatureRecords = new TreeSet<>(Comparator.comparing(TemperatureRecord::getDay));
}
