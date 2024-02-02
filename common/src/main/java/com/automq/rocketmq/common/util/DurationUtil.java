/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.rocketmq.common.util;

import java.time.Duration;

public class DurationUtil {
    public static Duration parse(String durationStr) {
        String perfix = "P";
        if (durationStr.contains("d")) {
            durationStr = durationStr.replace("d", "DT");
        } else if (durationStr.contains("D")) {
            durationStr = durationStr.replace("D", "DT");
        } else {
            perfix = "PT";
        }

        String iso8601 = perfix.concat(durationStr).toUpperCase();

        if (iso8601.endsWith("T")) {
            iso8601 = iso8601.concat("0S");
        }
        return Duration.parse(iso8601);
    }
}
