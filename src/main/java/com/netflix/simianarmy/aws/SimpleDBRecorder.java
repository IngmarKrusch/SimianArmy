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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;
import com.netflix.simianarmy.basic.BasicRecorderEvent;
import com.netflix.simianarmy.client.aws.AWSClient;

/**
 * The Class SimpleDBRecorder. Records events to and fetched events from a Amazon SimpleDB table (default SIMIAN_ARMY)
 */
public class SimpleDBRecorder extends AbstractRecorder {

    private final AmazonSimpleDB simpleDBClient;

    /**
     * Instantiates a new simple db recorder.
     *
     * @param awsClient
     *            the AWS client
     * @param domain
     *            the domain
     */
    public SimpleDBRecorder(AWSClient awsClient, String domain) {
        super(awsClient, domain);
        this.simpleDBClient = awsClient.sdbClient();
    }

    /**
     * simple client. abstracted to aid testing
     *
     * @return the amazon simple db
     */
    protected AmazonSimpleDB sdbClient() {
        return simpleDBClient;
    }


    /** {@inheritDoc} */
    @Override
    public void recordEvent(Event evt) {
        String evtTime = String.valueOf(evt.eventTime().getTime());
        List<ReplaceableAttribute> attrs = new LinkedList<ReplaceableAttribute>();
        attrs.add(new ReplaceableAttribute(Keys.id.name(), evt.id(), true));
        attrs.add(new ReplaceableAttribute(Keys.eventTime.name(), evtTime, true));
        attrs.add(new ReplaceableAttribute(Keys.region.name(), evt.region(), true));
        attrs.add(new ReplaceableAttribute(Keys.recordType.name(), "MonkeyEvent", true));
        attrs.add(new ReplaceableAttribute(Keys.monkeyType.name(), enumToValue(evt.monkeyType()), true));
        attrs.add(new ReplaceableAttribute(Keys.eventType.name(), enumToValue(evt.eventType()), true));
        for (Map.Entry<String, String> pair : evt.fields().entrySet()) {
            if (pair.getValue() == null || pair.getValue().equals("") || Keys.KEYSET.contains(pair.getKey())) {
                continue;
            }
            attrs.add(new ReplaceableAttribute(pair.getKey(), pair.getValue(), true));
        }
        // Let pk contain the timestamp so that the same resource can have multiple events.
        String pk = String.format("%s-%s-%s-%s", evt.monkeyType().name(), evt.id(), getRegion(), evtTime);
        PutAttributesRequest putReq = new PutAttributesRequest(getDomain(), pk, attrs);
        sdbClient().putAttributes(putReq);

    }

    /**
     * Find events.
     *
     * @param queryMap
     *            the query map
     * @param after
     *            the start time to query for all events after
     * @return the list
     */
    protected List<Event> findEvents(Map<String, String> queryMap, long after) {
        StringBuilder query = new StringBuilder(
                String.format("select * from %s where region = '%s'", getDomain(), getRegion()));
        for (Map.Entry<String, String> pair : queryMap.entrySet()) {
            query.append(String.format(" and %s = '%s'", pair.getKey(), pair.getValue()));
        }
        query.append(String.format(" and eventTime > '%d'", after));
        // always return with most recent record first
        query.append(" order by eventTime desc");

        List<Event> list = new LinkedList<Event>();
        SelectRequest request = new SelectRequest(query.toString());
        request.setConsistentRead(Boolean.TRUE);

        SelectResult result = new SelectResult();
        do {
            result = sdbClient().select(request.withNextToken(result.getNextToken()));
            for (Item item : result.getItems()) {
                Map<String, String> fields = new HashMap<String, String>();
                Map<String, String> res = new HashMap<String, String>();
                for (Attribute attr : item.getAttributes()) {
                    if (Keys.KEYSET.contains(attr.getName())) {
                        res.put(attr.getName(), attr.getValue());
                    } else {
                        fields.put(attr.getName(), attr.getValue());
                    }
                }
                String eid = res.get(Keys.id.name());
                String ereg = res.get(Keys.region.name());
                Enum monkeyType = valueToEnum(res.get(Keys.monkeyType.name()));
                Enum eventType = valueToEnum(res.get(Keys.eventType.name()));
                long eventTime = Long.parseLong(res.get(Keys.eventTime.name()));
                list.add(new BasicRecorderEvent(monkeyType, eventType, ereg, eid, eventTime).addFields(fields));
            }
        } while (result.getNextToken() != null);
        return list;
    }
}
