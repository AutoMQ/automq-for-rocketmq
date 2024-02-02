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

package com.automq.rocketmq.cli.tools;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

public class CliUtils {
    public static Throwable getRealException(Throwable throwable) {
        if (throwable instanceof CompletionException || throwable instanceof ExecutionException) {
            if (throwable.getCause() != null) {
                throwable = throwable.getCause();
            }
        }
        return throwable;
    }
}
