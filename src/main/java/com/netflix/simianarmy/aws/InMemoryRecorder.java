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

//        dumpEvents("recordEvent");
    }

//    private void dumpEvents(String tagline) {
//        System.out.println("======================== " + tagline + " ========================");
//        for (Iterator<String> iterator = events.keySet().iterator(); iterator.hasNext();) {
//            String id = iterator.next();
//            dumpEvent(events.get(id));
//        }
//    }
//
//    private void dumpEvent(Event event) {
//        System.out.println("id = " + event.id());
//        System.out.println("region = " + event.region());
//        System.out.println("eventTime = " + event.eventTime());
//        System.out.println("eventType = " + event.eventType());
//        System.out.println("monkeyType = " + event.monkeyType());
//        dumpMap("", event.fields());
//    }
//
//    private void dumpMap(String tag, Map<String, String> fields) {
//        if (tag.length() > 0) { System.out.println(tag); }
//        for (Iterator<String> iterator = fields.keySet().iterator(); iterator.hasNext();) {
//            String key = iterator.next();
//            System.out.println(key + " = " + fields.get(key));
//        }
//    }

    @Override
    protected List<Event> findEvents(Map<String, String> queryMap, long after) {
//        dumpEvents("findEvents");
//        dumpMap("=========== queryMap ===========", queryMap);

        LinkedList<Event> found = new LinkedList<Event>();
        Set<String> queryKeys = queryMap.keySet();
        for (Iterator<Event> iterator = events.values().iterator(); iterator.hasNext();) {
            Event event = iterator.next();
            boolean stillGood = event.eventTime().getTime() > after;
            if (stillGood) {
                for (String queryKey : queryKeys) {
                    String queryValue = queryMap.get(queryKey);

                    if (queryKey.equals("monkeyType")) {
                        stillGood &= queryValue.startsWith(event.monkeyType().name());
                        // why oh why does the queryMap contains this??
                        //CHAOS|com.netflix.simianarmy.chaos.ChaosMonkey$Type
                        //CHAOS_TERMINATION|com.netflix.simianarmy.chaos.ChaosMonkey$EventTypes
                        // so this does not work.
                        //stillGood &= ChaosMonkey.Type.valueOf(queryValue).equals(event.monkeyType());
                    } else if (queryKey.equals("eventType")) {
                        stillGood &= queryValue.startsWith(event.eventType().name());
                    } else if (queryKey.equals("id")) {
                        stillGood &= event.id().equalsIgnoreCase(queryValue);
                    } else if (queryKey.equals("region")) {
                        stillGood &= event.region().equalsIgnoreCase(queryValue);
                    } else {
                        String eventValue = event.field(queryKey);
                        stillGood = stillGood && eventValue != null && eventValue.equalsIgnoreCase(queryValue);
                    }
                    if (!stillGood) {
                        break;
                    }
                }
            }
            if (stillGood) {
                found.add(event);
            }
        }
//        System.out.println("found " + found.size());
        return found;
    }

}
