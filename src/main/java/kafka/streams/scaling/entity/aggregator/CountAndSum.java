package kafka.streams.scaling.entity.aggregator;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CountAndSum {

  private long count;
  private double sum;
}
