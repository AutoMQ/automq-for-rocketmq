/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.automq.stream.s3.metrics.wrapper;

import com.automq.stream.s3.metrics.MetricsConfig;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.yammer.metrics.core.MetricName;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class MetricsWrapperTest {

    @Test
    public void testConfigurableMetrics() {
        CounterMetric metric = new CounterMetric(new MetricsConfig(), Attributes.builder().put("extra", "v").build(),
            Mockito.mock(LongCounter.class));
        Assertions.assertEquals(MetricsLevel.INFO, metric.metricsLevel);

        metric.onConfigChange(new MetricsConfig(MetricsLevel.DEBUG, Attributes.builder().put("base", "v2").build()));
        Assertions.assertEquals(MetricsLevel.DEBUG, metric.metricsLevel);
        Assertions.assertEquals(Attributes.builder().put("extra", "v").put("base", "v2").build(), metric.attributes);

        YammerHistogramMetric yammerHistogramMetric = new YammerHistogramMetric(Mockito.mock(MetricName.class), MetricsLevel.INFO, new MetricsConfig(),
            Attributes.builder().put("extra", "v").build());
        Assertions.assertEquals(MetricsLevel.INFO, yammerHistogramMetric.metricsLevel);

        yammerHistogramMetric.onConfigChange(new MetricsConfig(MetricsLevel.DEBUG, Attributes.builder().put("base", "v2").build()));
        Assertions.assertEquals(MetricsLevel.DEBUG, yammerHistogramMetric.metricsLevel);
        Assertions.assertEquals(Attributes.builder().put("extra", "v").put("base", "v2").build(), yammerHistogramMetric.attributes);
    }

    @Test
    public void testMetricsLevel() {
        CounterMetric metric = new CounterMetric(new MetricsConfig(MetricsLevel.INFO, null), Mockito.mock(LongCounter.class));
        Assertions.assertTrue(metric.add(MetricsLevel.INFO, 1));
        Assertions.assertFalse(metric.add(MetricsLevel.DEBUG, 1));
        metric.onConfigChange(new MetricsConfig(MetricsLevel.DEBUG, null));
        Assertions.assertTrue(metric.add(MetricsLevel.INFO, 1));
        Assertions.assertTrue(metric.add(MetricsLevel.DEBUG, 1));

        YammerHistogramMetric yammerHistogramMetric = new YammerHistogramMetric(Mockito.mock(MetricName.class), MetricsLevel.INFO, new MetricsConfig(),
            Attributes.builder().put("extra", "v").build());
        Assertions.assertTrue(yammerHistogramMetric.shouldRecord());
        yammerHistogramMetric.onConfigChange(new MetricsConfig(MetricsLevel.DEBUG, null));
        Assertions.assertTrue(yammerHistogramMetric.shouldRecord());
        yammerHistogramMetric = new YammerHistogramMetric(Mockito.mock(MetricName.class), MetricsLevel.DEBUG, new MetricsConfig(),
            Attributes.builder().put("extra", "v").build());
        Assertions.assertFalse(yammerHistogramMetric.shouldRecord());
        yammerHistogramMetric.onConfigChange(new MetricsConfig(MetricsLevel.DEBUG, null));
        Assertions.assertTrue(yammerHistogramMetric.shouldRecord());
    }
}
