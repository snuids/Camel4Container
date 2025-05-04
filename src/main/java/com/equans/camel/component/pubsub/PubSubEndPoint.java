package com.equans.camel.component.pubsub;

import com.equans.pubsub.client.PubSubClient;
import jakarta.jms.JMSException;
import org.apache.camel.Component;
import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.api.management.ManagedAttribute;
import org.apache.camel.api.management.ManagedResource;
import org.apache.camel.support.DefaultEndpoint;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@ManagedResource(description = "Managed CofelyPubSubEndpoint")
@UriEndpoint(scheme = "pubsub", syntax = "pubsub:destinationName", title = "PubSub Component")
public class PubSubEndPoint extends DefaultEndpoint {

    private final PubSubClient pubSub;
    private final Map<String, String> statusValues;
    private String headersPrefix = "";
    private final Map<String, List<PubSubConsumer>> consumerTopics;
    private final Map<String, List<PubSubConsumer>> consumerQueues;
    private String action = "";

    @UriParam
    private final boolean onlyWhenLocalMaster;

    private static final Logger LOG = LoggerFactory.getLogger(PubSubEndPoint.class);

    public PubSubEndPoint(PubSubClient pubSub, Map<String, List<PubSubConsumer>> consumerTopics,
                          Map<String, List<PubSubConsumer>> consumerQueues, Map<String, String> statusValues, 
                          String prefix, String endpointUri, boolean onlyWhenLocalMaster, Component component) throws Exception {
        super(endpointUri, component);

        this.pubSub = pubSub;
        this.headersPrefix = prefix;
        this.statusValues = statusValues;
        this.consumerTopics = consumerTopics;
        this.consumerQueues = consumerQueues;
        this.onlyWhenLocalMaster = onlyWhenLocalMaster;

        int indexOfSlash = endpointUri.indexOf("/");
        this.action = endpointUri.substring(indexOfSlash + 2);
        this.action = URLDecoder.decode(this.action, "UTF-8");

        int indexOfMark = this.action.indexOf("?");
        if (indexOfMark > 0) {
            this.action = this.action.substring(0, indexOfMark);
        }
    }

    @ManagedAttribute(description = "Endpoint State")
    public String getState() {
        return getStatus().name();
    }

    @ManagedAttribute(description = "Camel ID")
    public String getCamelId() {
        return getCamelContext().getName();
    }

    @ManagedAttribute(description = "Camel ManagementName")
    public String getCamelManagementName() {
        return getCamelContext().getManagementName();
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
    }

    public Producer createProducer() throws Exception {
        return new PubSubProducer(pubSub, this.getEndpointUri(), statusValues, headersPrefix, this);
    }

    public Consumer createConsumer(Processor prcsr) throws Exception {
        PubSubConsumer c = null;
        if (action.contains("queue:")) {
            String queue = action.replace("queue:", "");
            c = createQueueConsumer(queue, prcsr);
        } else if (action.contains("topic:")) {
            String topic = action.replace("topic:", "");
            c = createTopicConsumer(topic, prcsr);
        } else {
            c = createTopicConsumer(action, prcsr);
        }
        return c;
    }

    private PubSubConsumer createTopicConsumer(String topic, Processor prcsr) throws JMSException {
        synchronized (this.consumerTopics) {
            if (!this.consumerTopics.containsKey(topic)) {
                this.consumerTopics.put(topic, new ArrayList<>());
                this.pubSub.subscribeTopic(topic);
            }

            PubSubConsumer c = new PubSubConsumer(this, prcsr);
            c.setOnlyWhenIsLocalMaster(onlyWhenLocalMaster);
            this.consumerTopics.get(topic).add(c);
            return c;
        }
    }

    private PubSubConsumer createQueueConsumer(String queue, Processor prcsr) throws JMSException {
        synchronized (this.consumerQueues) {
            if (!this.consumerQueues.containsKey(queue)) {
                this.consumerQueues.put(queue, new ArrayList<>());
                this.pubSub.subscribeQueue(queue);
            }

            PubSubConsumer c = new PubSubConsumer(this, prcsr);
            c.setOnlyWhenIsLocalMaster(onlyWhenLocalMaster);
            this.consumerQueues.get(queue).add(c);
            return c;
        }
    }

    public boolean isSingleton() {
        return false;
    }
}
