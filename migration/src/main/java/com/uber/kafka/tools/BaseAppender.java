// Copyright (c) 2015 Uber Technologies, Inc. All rights reserved.
// @author Seung-Yeoul Yang (syyang@uber.com)

package com.uber.kafka.tools;

import org.apache.log4j.AppenderSkeleton;

public abstract class BaseAppender extends AppenderSkeleton {
    protected final MigrationContext context;

    public BaseAppender(MigrationContext context) {
        this.context = context;
    }

    @Override
    public void close() { }

    @Override
    public boolean requiresLayout() { return false; }
}
