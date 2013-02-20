package org.nts.pigutils.proto;

import java.util.List;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

/**
 * Functional list utilities that google collections is for some reason lacking.
 */

public class ListHelper {
  public static <K> List<K> filter(List<K> input, Predicate<K> predicate) {
    List<K> output = Lists.newArrayList();
    for (K val: input) {
      if (predicate.apply(val)) {
        output.add(val);
      }
    }
    return output;
  }
}