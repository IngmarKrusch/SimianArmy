/*
 *
 *  Copyright 2012 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.simianarmy.aws;

import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.Validate;

import com.netflix.simianarmy.MonkeyRecorder;
import com.netflix.simianarmy.basic.BasicRecorderEvent;
import com.netflix.simianarmy.client.aws.AWSClient;

/**
 * The Class SimpleDBRecorder. Records events to and fetched events from a Amazon SimpleDB table (default SIMIAN_ARMY)
 */
@SuppressWarnings("serial")
public abstract class AbstractRecorder implements MonkeyRecorder {

    private final String region;

    /** The domain. */
    private final String domain;

    public String getRegion() {
        return region;
    }

    public String getDomain() {
        return domain;
    }

    /**
     * The Enum Keys.
     */
    protected enum Keys {

        /** The event id. */
        id,
        /** The event time. */
        eventTime,
        /** The region. */
        region,
        /** The record type. */
        recordType,
        /** The monkey type. */
        monkeyType,
        /** The event type. */
        eventType;

        /** The Constant KEYSET. */
        public static final Set<String> KEYSET = Collections.unmodifiableSet(new HashSet<String>() {
            {
                for (Keys k : Keys.values()) {
                    add(k.toString());
                }
            }
        });
    };

    /**
     * Instantiates a new simple db recorder.
     *
     * @param awsClient
     *            the AWS client
     * @param domain
     *            the domain
     */
    public AbstractRecorder(AWSClient awsClient, String domain) {
        Validate.notNull(awsClient);
        Validate.notNull(domain);
        this.region = awsClient.region();
        this.domain = domain;
    }

    /**
     * Enum to value. Converts an enum to "name|type" string
     *
     * @param e
     *            the e
     * @return the string
     */
    protected static String enumToValue(Enum e) {
        return String.format("%s|%s", e.name(), e.getClass().getName());
    }

    /**
     * Value to enum. Converts a "name|type" string back to an enum.
     *
     * @param value
     *            the value
     * @return the enum
     */
    @SuppressWarnings("unchecked")
    protected static Enum valueToEnum(String value) {
        // parts = [enum value, enum class type]
        String[] parts = value.split("\\|", 2);
        if (parts.length < 2) {
            throw new RuntimeException("value " + value + " does not appear to be an internal enum format");
        }

        Class enumClass;
        try {
            enumClass = Class.forName(parts[1]);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("class for enum value " + value + " not found");
        }
        if (enumClass.isEnum()) {
            final Class<? extends Enum> enumSubClass = enumClass.asSubclass(Enum.class);
            return Enum.valueOf(enumSubClass, parts[0]);
        }
        throw new RuntimeException("value " + value + " does not appear to be an enum type");
    }

    /** {@inheritDoc} */
    @Override
    public Event newEvent(Enum monkeyType, Enum eventType, String reg, String id) {
        return new BasicRecorderEvent(monkeyType, eventType, reg, id);
    }

    /**
     * TOxDO javadoc.
     *
     * @param queryMap
     * @param after
     * @return
     */
    protected abstract List<Event> findEvents(Map<String, String> queryMap, long after);

    /** {@inheritDoc} */
    @Override
    public List<Event> findEvents(Map<String, String> query, Date after) {
        return findEvents(query, after.getTime());
    }

    /** {@inheritDoc} */
    @Override
    public List<Event> findEvents(Enum monkeyType, Map<String, String> query, Date after) {
        Map<String, String> copy = new LinkedHashMap<String, String>(query);
        copy.put(Keys.monkeyType.name(), enumToValue(monkeyType));
        return findEvents(copy, after);
    }

    /** {@inheritDoc} */
    @Override
    public List<Event> findEvents(Enum monkeyType, Enum eventType, Map<String, String> query, Date after) {
        Map<String, String> copy = new LinkedHashMap<String, String>(query);
        copy.put(Keys.monkeyType.name(), enumToValue(monkeyType));
        copy.put(Keys.eventType.name(), enumToValue(eventType));
        return findEvents(copy, after);
    }
}
