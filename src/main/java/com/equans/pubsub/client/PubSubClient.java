package com.equans.pubsub.client;

import com.equans.pubsub.client.interfaces.IPubSubClientReceiver;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import jakarta.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

/**
 * This class emulate a BagStage pub sub client with - dynamic publishing topic
 * feature - listener for a given set of topics - listener for master or slave
 * status - properties retrieving feature
 */
public class PubSubClient implements MessageListener {

    private String PubSubVersion = "1.3";
    private final String topicStatus = "STATUS";
    private boolean ignoreStatusReception = false;
    
    private final String queueControl = "PUBSUB_CONTROL";
    private String queueReturn = "";
    
    private final Logger log = Logger.getLogger(PubSubClient.class);
    
    private final GuidGenerator generator;
    private final Runtime runtime = Runtime.getRuntime();
    private final DateTime startTime = DateTime.now();
    
    private boolean isLocalMaster = false;
    private DateTime localMasterSince;
    private DateTime lastOtherLocalMasterSince = DateTime.now();
    private boolean askToBecomeSlave = false;

    private String internalStatus = "Alive";
    
    private Connection connectionConsumer;
    private Connection connectionProducer;
    private Session sessionConsumer;
    private Session sessionProducer;
    private MessageProducer producer;
    private boolean isConnected = false;

    private final IPubSubClientReceiver listener;

    private String password = null;
    private String login = null;

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public String getLogin()
    {
        return login;
    }

    public void setLogin(String login)
    {
        this.login = login;
    }
    
    private String subscriberName = null;
    private String subscriberGroup = null;
    private String pubSubAddress = null;
    private String programVersion = null;
    private int statusInterval = 30;
    private List<String> topics = new ArrayList<String>();
    private List<String> queues = new ArrayList<String>();

    private Thread thread;
    private Thread threadStatus;
    private final List<String> resynchTodos = new ArrayList<String>();

    private SimpleDateFormat formatLaunchTime = new SimpleDateFormat("MM/dd/yyyy hh:mm a");

    /**
     * @return the subscriberName
     */
    public String getSubscriberName() {
        return subscriberName;
    }

    /**
     * @param subscriberName the subscriberName to set
     */
    public void setSubscriberName(String subscriberName) {
        this.subscriberName = subscriberName;
    }

    /**
     * @return the subscriberGroup
     */
    public String getSubscriberGroup() {
        return subscriberGroup;
    }

    /**
     * @param subscriberGroup the subscriberGroup to set
     */
    public void setSubscriberGroup(String subscriberGroup) {
        this.subscriberGroup = subscriberGroup;
    }

    /**
     * @return the programVersion
     */
    public String getProgramVersion() {
        return programVersion;
    }

    /**
     * @param programVersion the programVersion to set
     */
    public void setProgramVersion(String programVersion) {
        this.programVersion = programVersion;
    }

    public long getPubSubId() {
        return this.generator.getServerId();
    }

    public enum STATE {
        MASTER,
        SLAVE
    };
    
    public enum JMS_STATUS {
        CONNECTED,
        DISCONNECTED
    };

    public PubSubClient(String subscriberName, String subscriberGroup, String pubSubAddress, String subs, int statusInterval, String programVersion, IPubSubClientReceiver listener)
    {
       this( subscriberName, subscriberGroup, pubSubAddress, subs, statusInterval, programVersion, listener,null,null);
    }
    
    public PubSubClient(String subscriberName, String subscriberGroup, String pubSubAddress
            , String subs, int statusInterval, String programVersion, IPubSubClientReceiver listener
            ,String login,String password) {
        
        log.info("Creation PubSub Client Version:"+PubSubVersion);
        log.info("Address:"+pubSubAddress);
        this.subscriberName = subscriberName;
        this.generator = new GuidGenerator(subscriberName);
        this.queueReturn = this.subscriberName.replace(" ", "_")+ "_CONTROL";
        
        this.subscriberGroup = subscriberGroup;
        this.pubSubAddress = pubSubAddress;
        this.statusInterval = statusInterval;
        this.programVersion = programVersion;
        this.listener = listener;
        if (subs != null) {
            for(String s : Arrays.asList(subs.split(","))) {
                if (!s.isEmpty()) {
                    if (s.startsWith("queue:")){
                        String q = s.replace("queue:", "");
                        this.queues.add(q);
                    } else {
                        String t = s.replace("topic:", "");
                        this.topics.add(t);
                    }
                }
            }
        }
        
        try {
            this.listener.onStateChanged(STATE.SLAVE); // we init as slave
            this.isLocalMaster = false;
        } catch (Exception e) {
            log.error("[PubSubClient] Unable to set initial client as slave, error is " + e.toString());
        }
        
        try {
            this.listener.onJMSStatusChanged(JMS_STATUS.DISCONNECTED);
            this.isConnected = false;
        } catch (Exception e){
            log.warn("[PubSubClient] unable to call onJMSStatusChanged, error is " + e.toString());
        }
    }
    
    public void disableStatusListening() {
        this.ignoreStatusReception = true;
        
    }
    
    /**
     * @return the statusInterval
     */
    public int getStatusInterval() {
        return statusInterval;
    }

    /**
     * @param statusInterval the statusInterval to set
     */
    public void setStatusInterval(int statusInterval) {
        this.statusInterval = statusInterval;
    }
    
    public void setVersion(String camelVersion) {
        PubSubVersion=camelVersion;
    }

    public String getStatus() {
        return internalStatus;
    }

    public void setStatus(String status) {
        
        if (status == null) {
            log.warn("[setStatus] cannot set null status");
        }
        
        this.internalStatus = status.replace("|", "");
    }

    public void start() {

        try {
            
            int randomNum = 0 + (int)(Math.random()*100);
            
            log.info("[start] Trying to connect to " + "tcp://" + this.pubSubAddress + ":61616");
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://" + this.pubSubAddress + ":61616");
            
            if(login!=null)
            {
                log.info("Login:"+login);
                connectionFactory.setUserName(login);
            }
            else
                log.info("No Login Specified...");

            if(password!=null)
            {
                connectionFactory.setPassword(password);
                log.info("PasswordLength:"+password.length());
            }
            else
            {                
                log.info("No Password Specified...");
            }

            // Create a ConnectionFactory
            if (connectionProducer == null) {
                
                connectionFactory.setClientID(this.getSubscriberName() + "-PRODUCER-"+ randomNum);
                // Create a connection for producer
                this.connectionProducer = connectionFactory.createConnection();
                this.connectionProducer.setExceptionListener(new ExceptionListener() {
                    @Override
                    public void onException(JMSException jmse) {
                        log.error("[jmsException] error occurs: " + jmse.toString(), jmse);
                        try {
                            producer.close();
                        } catch (Exception e) { 
                        }
                        producer = null;
                        try {
                            sessionProducer.close();
                        } catch (Exception e) { 
                        }
                        sessionProducer = null;
                        try {
                            connectionProducer.close();
                        } catch (Exception e) { 
                        }
                        connectionProducer = null;
                        if (isConnected == true) {
                            isConnected = false;
                            try {
                                listener.onJMSStatusChanged(JMS_STATUS.DISCONNECTED);
                            } catch (Exception e){
                                log.warn("[start] unable to call onJMSStatusChanged for connectionProducer, error is " + e.toString());
                            }
                        }
                    }
                });
                this.connectionProducer.start();
                this.sessionProducer = this.connectionProducer.createSession(false, Session.AUTO_ACKNOWLEDGE);
                this.producer = this.sessionProducer.createProducer(null);
                this.producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            }
            
            if (this.connectionConsumer == null){

                // Create a Connection for consummer
                connectionFactory.setClientID(this.getSubscriberName() + "-CONSUMER-" + randomNum);
                this.connectionConsumer = connectionFactory.createConnection();
                this.connectionConsumer.setExceptionListener(new ExceptionListener() {
                    @Override
                    public void onException(JMSException jmse) {
                        log.error("[jmsException] error occurs: " + jmse.toString(), jmse);
                        try {
                            sessionConsumer.close();
                        } catch (Exception e) { 
                        }
                        sessionConsumer = null;
                        try {
                            connectionConsumer.close();
                        } catch (Exception e) { 
                        }
                        connectionConsumer = null;
                        if (isConnected == true){
                            isConnected = false;
                            try {
                                listener.onJMSStatusChanged(JMS_STATUS.DISCONNECTED);
                            } catch (Exception e){
                                log.warn("[start] unable to call onJMSStatusChanged for connectionConsumer, error is " + e.toString());
                            }
                        }
                    }
                });
                this.connectionConsumer.start();
                this.sessionConsumer = connectionConsumer.createSession(false, Session.AUTO_ACKNOWLEDGE);

                for (String topic : this.topics) {
                    this.sessionConsumer.createConsumer(this.sessionConsumer.createTopic(topic)).setMessageListener(this);
                }

                for (String queue : this.queues) {
                    this.sessionConsumer.createConsumer(this.sessionConsumer.createQueue(queue)).setMessageListener(this);
                }
                this.sessionConsumer.createConsumer(this.sessionConsumer.createQueue(this.queueReturn)).setMessageListener(this);
                
                if (this.ignoreStatusReception == false){
                    this.sessionConsumer.createConsumer(this.sessionConsumer.createTopic(this.topicStatus)).setMessageListener(this);
                }
            
            }
            
            try {
                this.listener.onJMSStatusChanged(JMS_STATUS.CONNECTED);
            } catch (Exception e){
                log.warn("[start] unable to call onJMSStatusChanged, error is " + e.toString());
            }
            this.isConnected = true;
            
            log.info("[start] Connection for producer and consumer succesfully established, asking for server id");
            
            // Ask for server id
            this.askForServerId();
            
            // We wait for init
            log.info("[start] waiting at maximun 30s for server id response");
            int maxNumber = 30000/50;
            while(maxNumber > 0) {
                Thread.sleep(20);
                maxNumber--;
                
                if (this.generator.getServerId() > 0) {
                    break;
                }
            }
            
            
        } catch (Exception e) {
            this.log.error("[start] Unable to start JMS Connection, exception is ", e);
        }

        if (this.thread == null) {
            log.debug("[start] Starting new pub sub task management");
            this.thread = new Thread(new PubSubClientTask());
            this.thread.setName("PubSubClientThread");
            this.thread.start();
        }

        if (this.threadStatus == null) {
            log.debug("[start] Starting new pub sub status task");
            this.threadStatus = new Thread(new PubSubClientStatus());
            this.threadStatus.setName("PubSubClientStatusThread");
            this.threadStatus.start();
        }
    }
    
    
    private void askForServerId() throws Exception, JMSException {
        // Creating server id message
        String ctn = "Action=AskServerID\r\nResynchQueue=" + this.queueReturn + "\r\n";
        ActiveMQTextMessage msg = new ActiveMQTextMessage();
        msg.setText(ctn);
        msg.setStringProperty("ClientName", this.getSubscriberName());
        msg.setLongProperty("GenerationTimeStamp", new Date().getTime());

        Destination dest = this.sessionProducer.createQueue(this.queueControl);

        synchronized (this.producer) {
            this.producer.send(dest, msg);
        }
        
    }

    public void onMessage(Message msg) {
        try {
            if (msg instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) msg;

                String body = textMessage.getText();
                String destination = textMessage.getJMSDestination().toString().replace("topic://", "").replace("queue://", "");
                
                log.trace("[onMessage] received message from " + destination + " with message " + body);

                if (this.queueReturn.equals(destination)) {
                    // Handle return queue
                    this.handleControlReturnQueue(body, msg);
                } else {
                    if (this.topicStatus.equals(destination)) {
                        this.handleMasterSlaveStatus(body);
                    }
                    if (this.listener != null) {
                        try {
                            Map<String, Object> properties = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
                            // We extract headers
                            Enumeration props = textMessage.getPropertyNames();
                            while(props.hasMoreElements()) {
                                String prop = (String) props.nextElement();
                                properties.put(prop, textMessage.getObjectProperty(prop));
                            }
                            this.listener.onMessageReceived(destination, body, properties);
                        } catch (Exception e) {
                            this.log.error("[onMessage] Error occur in listener message, error is " + e.toString());
                        }
                    } else {
                        this.log.warn("[onMessage] Message received but no listener set!");
                    }
                }
            } else if (msg instanceof BytesMessage) {
                
                Map<String, Object> properties = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
                
                BytesMessage bytesMessage = (BytesMessage) msg;
                byte[] data = new byte[(int) bytesMessage.getBodyLength()];
                bytesMessage.readBytes(data);
                
                // We extract headers
                Enumeration props = bytesMessage.getPropertyNames();
                while(props.hasMoreElements()) {
                    String prop = (String) props.nextElement();
                    properties.put(prop, bytesMessage.getObjectProperty(prop));
                }
                
                String destination = bytesMessage.getJMSDestination().toString().replace("topic://", "").replace("queue://", "");
                this.listener.onBinaryMessageReceived(destination, data, properties);
            }
        } catch (Exception e) {
            log.error("[onMessage] Unable to received message, error is " + e.toString());
        }
    }
      
    private void handleControlReturnQueue(String body, Message msg) {
        
        try {
            // Response to AskServerId
            if (body.startsWith("Action=SetServerID")) {
                Long id = Long.parseLong(msg.getStringProperty("ServerID"));
                
                log.info("[handleControlReturnQueue] receiving serverId :" + id);
                this.generator.setServerId(id);
                
            } else if (body.startsWith("Topic=")) {
                int index = body.indexOf('\r');
                if (index > 0) {
                    String topic = body.substring(6, index);
                    String mes = body.substring(index + 2);
                    this.listener.onMessageReceived(topic, mes, new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER));
                }
            }
        } catch (Exception e) {
            log.error("Unable to handle control queue, error is " + e.toString());
        }
    }
    
    private void handleMasterSlaveStatus(String body) {
        if (body.startsWith(this.getSubscriberName() + "|" + this.getSubscriberGroup())) {
            if (!body.startsWith(this.subscriberName + "|" + this.subscriberGroup+ "|" + this.generator.getServerId())) {

                this.log.debug("[handleMasterSlaveStatus] Other status for same application received ");
                if (body.contains("LOCALMASTER=1")) {

                    // the last master date
                    this.lastOtherLocalMasterSince = DateTime.now();

                    // case where we become slave because we received old message
                    int index = body.indexOf("LOCALMASTERSINCE=");
                    if (index > 0) {
                        index += "LOCALMASTERSINCE=".length();
                        int index2 = body.indexOf(",", index);
                        if (index2 > 0) {
                            String lmd = body.substring(index, index2);
                            Long time = (Long.parseLong(lmd) - 621355968000000000L)/10000;
                            DateTime otherLocalMasterSince = new DateTime(time);
                            
                            log.debug("[handleMasterSlaveStatus] other LM since " + otherLocalMasterSince.toString());

                            if (otherLocalMasterSince.equals(this.localMasterSince) 
                                    || otherLocalMasterSince.isBefore(this.localMasterSince)) {
                                if (isLocalMaster) {
                                    log.info("[handleMasterSlaveStatus] other is Master for more than current, becoming slave ");
                                    try {
                                        this.listener.onStateChanged(STATE.SLAVE);
                                        this.isLocalMaster = false;
                                    } catch (Exception e) {
                                        log.error("[handleMasterSlaveStatus] Unable to set slave, error is " + e.toString());
                                    }
                                }
                            }
                        }
                    }
                    // case where master is refused
                    if (body.contains("REFUSEMASTER=1")) {
                        log.warn("[handleMasterSlaveStatus] Other server refuse master...");
                        this.lastOtherLocalMasterSince = DateTime.now().minusYears(10);
                    }
                }
            }
        }
    }

    public void stop() {
        if (this.thread != null) {
            log.info("[stop] Stopping pubSubClient - task management");
            this.thread.interrupt();
            try {
                this.thread.join(5000);
            } catch (InterruptedException e) {
                log.error("[stop] Error while stopping", e);
            }
            this.thread = null;
        }
        if (this.threadStatus != null) {
            log.info("[stop] Stopping pubSubClient - status task");
            this.threadStatus.interrupt();
            try {
                this.threadStatus.join(5000);
            } catch (InterruptedException e) {
                log.error("[stop] Error while stopping", e);
            }
            this.threadStatus = null;
        }
        
        log.info("[stop] Closing JMS connections");
        try {
            this.connectionConsumer.close();
        } catch (Exception e) {
            this.log.error("[stop] Error during jms closing " + e.toString());
        }
        
        try {
            this.connectionProducer.close();
        } catch (Exception e) {
            this.log.error("[stop] Error during jms closing " + e.toString());
        }
    }

    public STATE getCurrentState() {
        if (isLocalMaster) {
            return STATE.MASTER;
        } else {
            return STATE.SLAVE;
        }
    }
    
    public void publishMessageOnQueue(String queue, Object message) {
        if (this.connectionProducer != null) {
            
            if (this.generator.getServerId() == 0) {
                this.log.warn("[publishMessageOnQueue] Cannot send message '"+message+"' because server number not knwow");
                return;
            }

            if (message == null){
                this.log.warn("[publishMessageOnQueue] Cannot send NULL message");
                return;
            }
            
            try {
                Message msg = null;
                
                if (message instanceof String){
                    msg = new ActiveMQTextMessage();
                    ((ActiveMQTextMessage)msg).setText((String)message);
                } else if (message instanceof byte[]) {
                    msg = new ActiveMQBytesMessage();
                    ((ActiveMQBytesMessage)msg).writeBytes((byte[])message);
                } else {
                   this.log.warn("[publishMessageOnTopic] Cannot send message becauce message type not handle " + message.getClass().toString());
                   return; 
                }
                msg.setStringProperty("ClientName", this.getSubscriberName());
                msg.setLongProperty("PubSubIDUTC", this.generator.nextUniqueID());
                msg.setLongProperty("GenerationTimeStamp", new Date().getTime());

                Destination dest = this.sessionProducer.createQueue(queue);

                synchronized (this.producer) {
                    this.producer.send(dest, msg);
                }

            } catch (Exception e) {
                this.log.error("[publishMessageOnQueue] Error when sending, error is", e);
            }
        } else {
            this.log.warn("[publishMessageOnQueue] Cannot send because JMS not connected");
        }

    }

    public void publishMessageOnQueue(String queue, Object message, Map<String, String> properties) {
        if (this.connectionProducer != null) {
            
            if (this.generator.getServerId() == 0) {
                this.log.warn("[publishMessageOnQueue] Cannot send message '"+message+"' because server number not knwow");
                return;
            }
            
            if (message == null){
                this.log.warn("[publishMessageOnQueue] Cannot send NULL message");
                return;
            }

            try {
                Message msg = null;
                
                if (message instanceof String){
                    msg = new ActiveMQTextMessage();
                    ((ActiveMQTextMessage)msg).setText((String)message);
                } else if (message instanceof byte[]) {
                    msg = new ActiveMQBytesMessage();
                    ((ActiveMQBytesMessage)msg).writeBytes((byte[])message);
                } else {
                   this.log.warn("[publishMessageOnTopic] Cannot send message becauce message type not handle " + message.getClass().toString());
                   return; 
                }
                
                msg.setStringProperty("ClientName", this.getSubscriberName());
                msg.setLongProperty("PubSubIDUTC", this.generator.nextUniqueID());
                msg.setLongProperty("GenerationTimeStamp", new Date().getTime());

                if (properties != null) {
                    // We remove innaceptable headers
                    this.cleanProperties(properties);
                    for (Entry<String, String> entry : properties.entrySet()) {
                        msg.setStringProperty(entry.getKey(), entry.getValue());
                    }
                }

                Destination dest = this.sessionProducer.createQueue(queue);

                synchronized (this.producer) {
                    this.producer.send(dest, msg);
                }

            } catch (Exception e) {
                this.log.error("[publishMessageOnQueue] Error when sending, error is", e);
            }
        } else {
            this.log.warn("[publishMessageOnQueue] Cannot send because JMS not connected");
        }

    }

    public void publishMessageOnTopic(String topic, Object message) {
        if (this.connectionProducer != null) {
            
            if (this.generator.getServerId() == 0) {
                this.log.warn("[publishMessageOnTopic] Cannot send message '"+message+"' because server number not knwow");
                return;
            }
            
            if (message == null){
                this.log.warn("[publishMessageOnTopic] Cannot send NULL message");
                return;
            }
            
            try {
                Message msg = null;
                
                if (message instanceof String){
                    msg = new ActiveMQTextMessage();
                    ((ActiveMQTextMessage)msg).setText((String)message);
                } else if (message instanceof byte[]) {
                    msg = new ActiveMQBytesMessage();
                    ((ActiveMQBytesMessage)msg).writeBytes((byte[])message);
                } else {
                   this.log.warn("[publishMessageOnTopic] Cannot send message becauce message type not handle " + message.getClass().toString());
                   return; 
                }
                
                msg.setStringProperty("ClientName", this.getSubscriberName());
                msg.setLongProperty("PubSubIDUTC", this.generator.nextUniqueID());
                msg.setLongProperty("GenerationTimeStamp", new Date().getTime());

                Destination dest = this.sessionProducer.createTopic(topic);

                synchronized (this.producer) {
                    this.producer.send(dest, msg);
                }

            } catch (Exception e) {
                this.log.error("[publishMessageOnTopic] Error when sending, error is", e);
            }

        } else {
            this.log.warn("[publishMessageOnTopic] Cannot send because JMS not connected");
        }

    }
    
    public void publishMessageOnTopic(String topic, Object message, Map<String, String> properties) {
        if (this.connectionProducer != null) {
            
            if (this.generator.getServerId() == 0) {
                this.log.warn("[publishMessageOnTopic] Cannot send message '"+message+"' because server number not knwow");
                return;
            }
            if (message == null){
                this.log.warn("[publishMessageOnTopic] Cannot send NULL message");
                return;
            }
            
            try {
                Message msg = null;
                
                if (message instanceof String){
                    msg = new ActiveMQTextMessage();
                    ((ActiveMQTextMessage)msg).setText((String)message);
                } else if (message instanceof byte[]) {
                    msg = new ActiveMQBytesMessage();
                    ((ActiveMQBytesMessage)msg).writeBytes((byte[])message);
                } else {
                   this.log.warn("[publishMessageOnTopic] Cannot send message becauce message type not handle " + message.getClass().toString());
                   return; 
                }
                
                msg.setStringProperty("ClientName", this.getSubscriberName());
                msg.setLongProperty("PubSubIDUTC", this.generator.nextUniqueID());
                msg.setLongProperty("GenerationTimeStamp", new Date().getTime());
                
                if (properties != null) {
                    // We remove innaceptable headers
                    this.cleanProperties(properties);
                    for (Entry<String, String> entry : properties.entrySet()) {
                        msg.setStringProperty(entry.getKey(), entry.getValue());
                    }
                }

                Destination dest = this.sessionProducer.createTopic(topic);

                synchronized (this.producer) {
                    this.producer.send(dest, msg);
                }

            } catch (Exception e) {
                this.log.error("[publishMessageOnTopic] Error when sending, error is", e);
            }

        } else {
            this.log.warn("[publishMessageOnTopic] Cannot send because JMS not connected");
        }

    }
    
    private void cleanProperties(Map<String, String> properties){
        properties.remove("ClientName");
        properties.remove("PubSubID");
        properties.remove("GenerationTimeStamp");
        properties.remove("IsResynch");
        
    }

    public void addResynch(String prop) {
        synchronized (this.resynchTodos) {
            log.info("[addResynch] adding " + prop);
            this.resynchTodos.add(prop);
        }
    }
    
    public void subscribeTopic(String topic) throws JMSException{
        
       if (topic == null || topic.isEmpty()) {
           log.warn("[subscribeTopic] invalid topic name");
           return;
       } 
       
       this.sessionConsumer.createConsumer(this.sessionConsumer.createTopic(topic)).setMessageListener(this); 
       this.topics.add(topic);
    }
    
    public void subscribeQueue(String queue) throws JMSException{
        
       if (queue == null || queue.isEmpty()) {
           log.warn("[subscribeQueue] invalid topic name");
           return;
       } 
       
       this.sessionConsumer.createConsumer(this.sessionConsumer.createQueue(queue)).setMessageListener(this); 
       this.queues.add(queue);
    }
    
    public void becomeSlave() {
        log.info("[becomeSlave] ask to become slave ");
        this.askToBecomeSlave = true;
        this.localMasterSince = DateTime.now().plusSeconds(2 * getStatusInterval());
    }

    private class PubSubClientTask implements Runnable {

        public void run() {
            try {
                while (true) {
                    try {
                        if (connectionConsumer == null || connectionProducer == null) {
                            log.info("[run] JMS not connected, starting process...");
                            start();
                        }

                        // we check master slave
                        if (isLocalMaster == false && lastOtherLocalMasterSince.plusSeconds(2*getStatusInterval()).isBeforeNow()) {
                            try {
                                log.info("[run] No status message received from other server, switching Master");
                                listener.onStateChanged(STATE.MASTER);
                                isLocalMaster = true;
                                localMasterSince = DateTime.now();
                            } catch (Exception e) {
                                log.error("[run] Unable to set master state, error is " + e.toString());
                            }
                        }
                        
                        // we check for resynch todo
                        List<String> resynchToDelete = new ArrayList<String>();
                        if (listener != null && generator.getServerId() > 0) {
                            SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyyHHmmssSSS");
                            synchronized (resynchTodos) {
                                for (String res : resynchTodos) {
                                    log.info("[run] resynchonizing " + res);
                                    
                                    String msg = "CurrentTime=" + getNowAsTick() + "," + sdf.format(new Date())
                                            + "\r\nTopics=" + res + "\r\nResynchQueue=" + queueReturn + "\r\nAction=Resynch";
                                    
                                    publishMessageOnQueue(queueControl, msg, null);
                                    
                                    resynchToDelete.add(res);
                                    
                                }
                            }
                            
                            // we remove sync done
                            synchronized (resynchTodos) {
                                for (String res : resynchToDelete) {
                                    resynchTodos.remove(res);
                                }
                            }
                        }

                        //we waint 5 seconds between each loop
                        Thread.sleep(5000);

                    } catch (Exception e) {
                        if (e instanceof InterruptedException) {
                            throw e;
                        } else {
                            log.error("[run] Unable to monitor pubsubclient, error is", e);
                        }
                    }
                }
            } catch (Exception e) {
                log.error("[run] Error is " + e.toString());
            }
        }

    }
    
    public long getNowAsTick(){
        return 621355968000000000L+System.currentTimeMillis() *10000;
    }

    private class PubSubClientStatus implements Runnable {
        public void run() {
            try {
                while (true) {
                    try {
                        
                        Thread.sleep(getStatusInterval() * 1000);
                        
                        // If not connected, we dont try to send status
                        if (isConnected == false) {
                            continue;
                        }
                        
                        if (generator.getServerId() == 0) {
                            log.warn("[run] Cannot send pubsub status because serverId is not known");
                            askForServerId();
                            continue;
                        } 
                        
                        log.debug("[run] Sending status for " + getSubscriberName() + "|" + getSubscriberGroup());
                        
                        // We send the status
                        String status = getSubscriberName() + "|" + getSubscriberGroup() + "|" + 
                                        generator.getServerId() + "|"  + 
                                        InetAddress.getLocalHost().getHostName() + "|" + 
                                        PubSubVersion + "|" +  getProgramVersion() + "|" + 
                                        ((runtime.totalMemory()-runtime.freeMemory())/1000000) + "|" + 
                                        "LOCALMASTER=" + (isLocalMaster? "1": "0");
                                        
                       
                        Map<String,String> customStatus = listener.onFillMessage();
                        if (customStatus != null) {
                            for(String key : customStatus.keySet()) {
                                status += "," + key + "=" + customStatus.get(key);
                            }
                        }
                        
                        if (isLocalMaster) {
                            long localSince = 621355968000000000L + localMasterSince.getMillis() * 10000;
                            status += ",LOCALMASTERSINCE=" + localSince;
                        } 
                        
                        if (askToBecomeSlave) {
                            status += ",REFUSEMASTER=1"; 
                            askToBecomeSlave = false;
                        }

                        status += ",LAUNCHTIME=" + formatLaunchTime.format(startTime.toDate()) 
                               + ",JAVAVERSION=" + System.getProperty("java.version")
                               + "|0|" + internalStatus;
                        
                        // we publish the status
                        publishMessageOnTopic("STATUS", status);

                    } catch (Exception e) {
                        if (e instanceof InterruptedException) {
                            throw e;
                        } else {
                            log.error("[run] Exception is " + e.toString());
                        }
                    }
                }
            } catch (Exception e) {
                log.error("[run] Error is " + e.toString());
            }
        }
    }

}
