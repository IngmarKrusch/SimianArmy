package com.netflix.simianarmy.aws;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.netflix.simianarmy.client.aws.AWSClient;


/**
 * Records events in memory. Read: no persistence!
 *
 * @author ingmar.krusch@immobilienscout24.de
 */
public class InMemoryRecorder  extends AbstractRecorder {
    private HashMap<String, Event> events = new HashMap<String, Event>();

    /**
     * Instantiates a new in memory recorder.
     *
     * @param awsClient
     *            the AWS client
     * @param domain
     *            the domain
     */
    public InMemoryRecorder(AWSClient awsClient, String domain) {
        super(awsClient, domain);
    }

    @Override
    public void recordEvent(Event evt) {
        events.put(evt.id(), evt);
    }

    @Override
    protected List<Event> findEvents(Map<String, String> queryMap, long after) {
        LinkedList<Event> found = new LinkedList<Event>();
        Set<String> queryKeys = queryMap.keySet();
        for (Iterator<Event> iterator = events.values().iterator(); iterator.hasNext();) {
            Event event = iterator.next();
            if (event.eventTime().getTime() > after) {
                for (String queryKey : queryKeys) {
                    String queryValue = queryMap.get(queryKey);
                    if (event.field(queryKey).equalsIgnoreCase(queryValue)) {
                        found.add(event);
                    }
                }
            }
        }
        return found;
    }

}
