package com.equans.camel.component.pubsub;

import com.equans.pubsub.client.PubSubClient;
import com.equans.pubsub.client.interfaces.IPubSubClientReceiver;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.support.DefaultExchange;
import org.apache.camel.support.DefaultComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;


public class PubSubComponent extends DefaultComponent implements IPubSubClientReceiver,InitializingBean {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubComponent.class);

    private PubSubClient pubSub = null;
    private Boolean isLocalMaster = false;

    private final Map<String, List<PubSubConsumer>> consumerTopics = new HashMap<>();
    private final Map<String, List<PubSubConsumer>> consumerQueues = new HashMap<>();

    private String subscriberName = "COFELY";
    private String subscriberGroup = "COFELY";
    private String pubSubAddress = "127.0.0.1";
    private String programVersion = "1.1";
    private String statusInterval = "30";
    private String headersPrefix = "JavaPubSub";
    private String login = null;
    private String password = null;

    private final Map<String, String> statusValues = new HashMap<>();
    private String camelVersion;

    public PubSubComponent() {
        this.camelVersion = programVersion;
        
        //if (this.pubSub != null) {
        //    this.pubSub.setSubscriberName(subscriberName);
        //}
    }
    
    /**
     * Initialize bean after property set.
     */
    @Override
    public void afterPropertiesSet() {
        LOG.info("PubSub Properties set...");
        this.initPubSubIfNull();
    }

    public PubSubClient getPubSub() {
        // Init if null
        this.initPubSubIfNull();
        return this.pubSub;
    }

    @Override
    protected Endpoint createEndpoint(String uri, final String remaining, Map<String, Object> parameters) throws Exception {
        LOG.debug("{} using schema resource: {}", this, uri);

        this.initPubSubIfNull();

        boolean onlyLocalMaster = getAndRemoveParameter(parameters, "OnlyLocalMaster", Boolean.class, false);

        return new PubSubEndPoint(this.pubSub, consumerTopics, consumerQueues, statusValues, headersPrefix, uri, onlyLocalMaster, this);
    }

    public void onMessageReceived(String dest, String message, Map<String, Object> properties) {
        this.processJmsIncoming(dest, message, properties);
    }

    public void onBinaryMessageReceived(String dest, byte[] message, Map<String, Object> properties) {
        this.processJmsIncoming(dest, message, properties);
    }

    private synchronized void initPubSubIfNull() {
        if (this.pubSub == null) {
            this.pubSub = new PubSubClient(subscriberName, subscriberGroup, pubSubAddress, ""
                    , Integer.parseInt(getStatusInterval()), programVersion, this,login,password);
            this.pubSub.setVersion(camelVersion);
            this.pubSub.start();
        }
    }

    private void processJmsIncoming(String dest, Object message, Map<String, Object> properties) {
        if (this.consumerTopics.containsKey(dest)) {
            Exchange exch = createExchange(dest, message, properties);
            for (PubSubConsumer c : consumerTopics.get(dest)) {
                if (c.needNotification(this.isLocalMaster)) {
                    try {
                        c.getProcessor().process(exch);
                    } catch (Exception ex) {
                        LOG.error("Error forwarding incoming topics", ex);
                    }
                }
            }
        }

        if (this.consumerQueues.containsKey(dest)) {
            Exchange exch = createExchange(dest, message, properties);
            for (PubSubConsumer c : consumerQueues.get(dest)) {
                if (c.needNotification(this.isLocalMaster)) {
                    try {
                        c.getProcessor().process(exch);
                    } catch (Exception ex) {
                        LOG.error("Error forwarding incoming queue", ex);
                    }
                }
            }
        }
    }

    private Exchange createExchange(String dest, Object message, Map<String, Object> properties) {
        Exchange exch = new DefaultExchange(getCamelContext());

        exch.getIn().setHeader("From", dest);
        if (this.pubSub != null) {
            exch.getIn().setHeader("PubSubId", this.pubSub.getPubSubId());
        }
        exch.getIn().setBody(message);

        for (String k : properties.keySet()) {
            exch.getIn().setHeader(k, properties.get(k));
        }
        return exch;
    }

    public void onStateChanged(PubSubClient.STATE state) {
        this.isLocalMaster = (state == PubSubClient.STATE.MASTER);
    }

    public void onJMSStatusChanged(PubSubClient.JMS_STATUS status) {
        // Do nothing as we are not evented
    }

    public Map<String, String> onFillMessage() {
        return this.statusValues;
    }

    // Getters and setters for all fields

    public String getSubscriberName() {
        return subscriberName;
    }

    public void setSubscriberName(String subscriberName) {
        this.subscriberName = subscriberName;
        
    }

    public String getSubscriberGroup() {
        return subscriberGroup;
    }

    public void setSubscriberGroup(String subscriberGroup) {
        this.subscriberGroup = subscriberGroup;
        if (this.pubSub != null) {
            this.pubSub.setSubscriberGroup(subscriberGroup);
        }
    }

    public String getLogin() {
        return login;
    }

    public void setLogin(String login) {
        this.login = login;
    }
    
    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
    
    
    public String getPubSubAddress() {
        return pubSubAddress;
    }

    public void setPubSubAddress(String pubSubAddress) {
        this.pubSubAddress = pubSubAddress;
    }

    public String getProgramVersion() {
        return programVersion;
    }

    public void setProgramVersion(String programVersion) {
        this.programVersion = programVersion;
        if (this.pubSub != null) {
            this.pubSub.setProgramVersion(programVersion);
        }
    }

    public String getStatusInterval() {
        return statusInterval;
    }

    public void setStatusInterval(String statusInterval) {
        this.statusInterval = statusInterval;
        if (this.pubSub != null) {
            this.pubSub.setStatusInterval(Integer.parseInt(getStatusInterval()));
        }
    }

    public String getHeadersPrefix() {
        return headersPrefix;
    }

    public void setHeadersPrefix(String headersPrefix) {
        this.headersPrefix = headersPrefix;
    }

    public void setCamelVersion(String camelVersion) {
        this.camelVersion = camelVersion;
        if (this.pubSub != null) {
            this.pubSub.setVersion(camelVersion);
        }
    }
}
