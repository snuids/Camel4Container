package pubsub;

import camelworker.CamelWorker;
import camelworker.CamelWorker.TopicsConfiguration;
import helper.GUIDGenerator;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.component.jms.JmsConfiguration;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pubsub.jmx.Supervisor;

/**
 * 1.7.5 Use [(CR)] instead of the Â¬ character<br/>
 * 1.8.0 Correct a bug that prevents the resynchronization messages to be sent
 * 1.8.1 IP Addressed added
 * in the correct order<br/>
 *
 * @author Arnaud Marchand
 *
 */
public class Dispatcher implements Processor {

    //private final static Sigar sigar = new Sigar();
    static final Logger logger = LoggerFactory.getLogger("Dispatcher");
    static final Runtime runtime = Runtime.getRuntime();
    // BEANS
    MasterHandler masterHandlerBean;
    BSSingleton singletonBean;
    PersistenceUnit fileAppenderBean;
    // TIMERS
    Timer statusTimer;
    Timer masterTimer;
    
    // PROPERTIES
    String persistentTopics;
    String singletonTopics;
    String singletonTopicsNoHistory;
    String standardTopics;
    String otherServers;

    LinkedHashMap<String, String> singletonsWithoutHistory = new LinkedHashMap<String, String>();

    int serverID;
    int statusInterval;
    Connection connection = null;
    
    // SINGLETON CONNECTION
    Connection singletonconnection = null;
    Session singletonsession = null;
    
    // PERSISTENT CONNECTION
    Connection persistentconnection = null;
    Session persistentsession = null;
    MessageProducer persistentproducer = null;
    
    // REPLICATION CONNECTION
    Connection replicationconnection = null;
    Session replicationsession = null;
    public Map<String, ReplicationMessageListener> replicationListenersPerTopic = new HashMap<String, ReplicationMessageListener>();
    public List<String> singletonConsumers = new ArrayList<String>();
    
    String computername;
    String version = "1.8.1.0";
    long numberOfMessagesResynched = 0;
    String persistentMessageQueue = "";
    String persistentMessageQueueSQLDateFormatter = "yyyy-MM-dd HH:mm:ss";
    SimpleDateFormat sqldateformatter = new SimpleDateFormat(persistentMessageQueueSQLDateFormatter);
    private Supervisor jmxSupervisor = null;
    
    // License
    public String sleutelHard = "";
    public String sleutelSoft = "";
    public String publicSleutel = "";
    String osName = "";
    String osArchitecture = "";
    long totalMem = 0;
    
    
    GUIDGenerator guidGenerator = new GUIDGenerator();

    List<InetAddress>   ipAddresses=new ArrayList<InetAddress>();
    
    public Dispatcher() {
        logger.info("Initializing PubSub dispatcher.");
        ipAddresses=getListOfIPsFromNIs();
    }
        
    
    
    public Supervisor getJmxSupervisor() {
        return jmxSupervisor;
    }

    public void setJmxSupervisor(Supervisor jmxSupervisor) {
        this.jmxSupervisor = jmxSupervisor;
    }

    public String getPersistentMessageQueueSQLDateFormatter() {
        return persistentMessageQueueSQLDateFormatter;
    }

    public void setPersistentMessageQueueSQLDateFormatter(String persistentMessageQueueSQLDateFormatter) {
        this.persistentMessageQueueSQLDateFormatter = persistentMessageQueueSQLDateFormatter;
        sqldateformatter = new SimpleDateFormat(persistentMessageQueueSQLDateFormatter);
    }

    public String getPersistentMessageQueue() {
        return persistentMessageQueue;
    }

    public void setPersistentMessageQueue(String persistentMessageQueue) {
        this.persistentMessageQueue = persistentMessageQueue;
    }

    public MasterHandler getMasterHandlerBean() {
        return masterHandlerBean;
    }

    public void setMasterHandlerBean(MasterHandler masterHandlerBean) {
        this.masterHandlerBean = masterHandlerBean;
        masterHandlerBean.serverID = this.serverID;
    }

    public BSSingleton getSingletonBean() {
        return singletonBean;
    }

    public void setSingletonBean(BSSingleton singletonBean) {
        this.singletonBean = singletonBean;
    }

    public PersistenceUnit getFileAppenderBean() {
        return fileAppenderBean;
    }

    public void setFileAppenderBean(PersistenceUnit fileAppenderBean) {
        this.fileAppenderBean = fileAppenderBean;
        this.fileAppenderBean.owner = this;
    }

    public int getStatusInterval() {
        return statusInterval;
    }

    public void setStatusInterval(int statusInterval) {
        this.statusInterval = statusInterval;
    }

    public String getPersistentTopics() {
        return persistentTopics;
    }

    public void setPersistentTopics(String persitentTopics) {
        this.persistentTopics = persitentTopics;
    }

    public int getServerID() {
        return serverID;
    }

    public void setServerID(int serverID) {
        this.serverID = serverID;
    }

    public String getSingletonTopics() {
        return singletonTopics;
    }

    public void setSingletonTopics(String singletonTopics) {
        this.singletonTopics = singletonTopics;
    }

    public void setSingletonTopicsNoHistory(String singletonTopicsNoHistory) {
        this.singletonTopicsNoHistory = singletonTopicsNoHistory;
    }

    public String getSingletonTopicsNoHistory() {
        return singletonTopicsNoHistory;
    }
    
    public String getStandardTopics() {
        return standardTopics;
    }
    
    public void setStandardTopics(String standardTopics) {
        this.standardTopics = standardTopics;
    }

    public String getOtherServers() {
        return otherServers;
    }

    public void setOtherServers(String otherServers) {
        this.otherServers = otherServers;
    }

    // Get list of IP addresses from all local network interfaces. (JDK1.7)
    // -----------------------------------------------------------
    public List<InetAddress> getListOfIPsFromNIs()
    {
        List<InetAddress> addrList           = new ArrayList<InetAddress>();
    
        try
        {
            Enumeration<NetworkInterface> enumNI = NetworkInterface.getNetworkInterfaces();
            while ( enumNI.hasMoreElements() ){
                NetworkInterface ifc             = enumNI.nextElement();
                if( ifc.isUp() ){
                    Enumeration<InetAddress> enumAdds     = ifc.getInetAddresses();
                    while ( enumAdds.hasMoreElements() ){
                        InetAddress addr                  = enumAdds.nextElement();
                        if(!addr.toString().contains(":"))
                            addrList.add(addr);
                        System.out.println(addr.getHostAddress());   //<---print IP
                    }
                }
            }
        }
        catch(Exception e)
        {
            logger.error(("Unable to retrieve IPs. Ex="+e.getMessage()));
        }
        return addrList;
    }
    
    public void startPubSub() {
        
        logger.info("Start PubSub");
        logger.info("ServerID:" + serverID);
        logger.info("Other Servers:" + otherServers);
        logger.info("Persistent Topics:" + persistentTopics);
        logger.info("Standard Topics:" + standardTopics);
        logger.info("Singleton Topics:" + singletonTopics);

        guidGenerator.setServerID(serverID);

        try {
            computername = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException ex) {
            computername = "NA";
        }
        logger.info("Computer Name:" + computername);

        CamelContext camelcontext = (CamelContext) camelworker.CamelWorker.context.getBean("camel");
        if (camelcontext == null) {
            logger.error("Camel context not found. Check that the Camel Context is named 'camel' in the xml configuration file.");
            return;
        }

        if (this.singletonBean != null) {
            // Prepare singletons directory
            try {
                String directoryname = this.prepareBaseDirectoryForSingleton();
                for (String sing : this.getSingletonTopics().split(",")) {
                    this.prepareDirectoryForSingleton(sing, directoryname);
                }
            } catch (Exception e) {
                logger.error("Unable to create directory. Ex=" + e.getMessage(), e);
            }
        }

        createSingletonJMSConnection();
        createPersistentJMSConnection();
        createReplicationJMSConnection();

        //**********************************************************************
        // Status Task
        //**********************************************************************
        class InnerTimerTask extends TimerTask {

            final SimpleDateFormat formatlaunchtime = new SimpleDateFormat("MM/dd/yyyy hh:mm a");
            final SimpleDateFormat format = new SimpleDateFormat("ddMMMyyyy HH:mm:ss");
            final SimpleDateFormat format2 = new SimpleDateFormat("ddMMyyyy HH:mm:ss.S");
            final CamelContext camelcontext = (CamelContext) camelworker.CamelWorker.context.getBean("camel");

            @Override
            public void run() {
                try {
                    logger.debug("Status triggered");
                    if (connection == null) {
                        createJMSConnection();
                    }
                    if (connection != null) {
                        Session sess = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                        Destination destination = sess.createTopic("STATUS");
                        MessageProducer producer = sess.createProducer(destination);
                        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                        TextMessage message;

                        StringBuilder mess = new StringBuilder();
                        synchronized (fileAppenderBean.messagesPerTopic) {
                            for (Entry<String, Long> ent : fileAppenderBean.messagesPerTopic.entrySet()) {
                                mess.append(ent.getKey() + "=" + ent.getValue() + ",");
                            }
                        }

                        // We get timeshift
                        long abstimeShift = getMasterHandlerBean().timeShift;
                        if (abstimeShift < 0) {
                            abstimeShift = abstimeShift * -1;
                        }

                        String timeshift = "";
                        String color = "Color=#080,";
                        String status = "Alive";

                        // We compute status and color 
                        if (!getMasterHandlerBean().isMaster()) {
                            timeshift = "TIMESHIFT=" + abstimeShift + " ms,";
                            if (abstimeShift > 2000) {
                                color = "Color=#F00,";
                                status = "Timeshift too high";
                            }

                        }
                        
                        String ips="";
                        int ipcount=1;
                        for(InetAddress add:ipAddresses)
                        {
                            ips+="IP_"+ipcount+"="+add.getHostAddress()+",";
                            ipcount++;
                        }
                        
                        message = sess.createTextMessage("Kernel|*APSB|" + serverID + "|"
                                + computername + "|" + CamelWorker.version + "|" + version + "|"
                                + ((runtime.totalMemory() - runtime.freeMemory()) / 1000000)
                                + "|STARTTIME=" + format.format(new Date(camelworker.CamelWorker.context.getStartupDate())) + ","
                                + "TIMESTAMP=" + format2.format(new Date()) + ","
                                + "LAUNCHTIME=" + formatlaunchtime.format(new Date(camelworker.CamelWorker.context.getStartupDate())) + ","
                                + "SYSUPTIME=" + "0" + ","
                                + timeshift
                                + color
                                +ips
                                + "JAVAVERSION=" + System.getProperty("java.version") + ","
                                + "CAMELVERSION=" + CamelWorker.camelVersion + ","
                                + "ROUTES=" + camelcontext.getRoutes().size() + ","
                                + "DISKWRITES=" + getFileAppenderBean().messagesWrittenToDisk + ","
                                + "FREEMEMORY=" + runtime.freeMemory() / 1000000 + " MB,"
                                + "TOTALMEMORY=" + runtime.totalMemory() / 1000000 + " MB,"
                                + "MAXMEMORY=" + runtime.maxMemory() / 1000000 + " MB,"
                                + "PROCESSORS=" + runtime.availableProcessors() + ","
                                + "COMPUTERMEMORY=" + totalMem + " MB,"
                                + "OS=" + osName + ","
                                + "ARCHITECTURE=" + osArchitecture + ","
                                + "JMXSTATUS=" + (jmxSupervisor != null ? "1" : "0") + ","
                                + "IDM=" + "0" + ","
                                + mess
                                + "TOTALRESYNCHS=" + numberOfMessagesResynched + ","
                                + "MASTER=" + (masterHandlerBean.isMaster() ? "1 (" + format.format(masterHandlerBean.masterSinceDate) + ")" : "0") + ","
                                + "|0|" + status);
                        message.setLongProperty("PubSubID", serverID);
                        producer.send(message);
                        producer.close();

                        if (jmxSupervisor != null) {
                            message = sess.createTextMessage(jmxSupervisor.toJson());
                            message.setLongProperty("PubSubID", serverID);

                            destination = sess.createTopic("ROUTESTATUS");
                            producer = sess.createProducer(destination);
                            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                            producer.send(message);
                            producer.close();
                        }
                        sess.close();
                    }
                } catch (Exception e) {
                    logger.error("Unable to trigger status. Ex=" + e.getMessage(), e);
                }

            }
        }
        statusTimer = new Timer("Status Timer", true);
        statusTimer.scheduleAtFixedRate(new InnerTimerTask(), new Date(), statusInterval * 1000L);
        //**********************************************************************

        //**********************************************************************
        // Master Task
        //**********************************************************************
        class InnerMasterTask extends TimerTask {

            final Calendar cal = Calendar.getInstance();

            @Override
            public void run() {
                try {
                    logger.debug("Master triggered");
                    if (singletonconnection == null) {
                        createSingletonJMSConnection();
                    }
                    if (persistentconnection == null) {
                        createPersistentJMSConnection();
                    }
                    if (replicationconnection == null) {
                        createReplicationJMSConnection();
                    }
                    Date curdate = new Date();
                    cal.setTime(masterHandlerBean.lastMasterReceived);
                    cal.add(Calendar.SECOND, 20);

                    if ((cal.getTime().before(curdate)) || (masterHandlerBean.Master)) {
                        logger.debug("Triggering Master");

                        if (connection == null) {
                            createJMSConnection();
                        }
                        if (connection != null) {
                            Session sess = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                            masterHandlerBean.setMaster(true);
                            Destination destination = sess.createTopic("MAS");
                            MessageProducer producer = sess.createProducer(destination);
                            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                            TextMessage message;

                            message = sess.createTextMessage("MASTER|" + serverID + "#" + masterHandlerBean.masterSinceDate.getTime());
                            //message.setLongProperty("PubSubID", serverID);
                            message.setStringProperty("ClientName", "Kernel");
                            message.setLongProperty("PubSubIDUTC", guidGenerator.nextUniqueID());

                            message.setLongProperty("GenerationTimeStamp", new Date().getTime());

                            producer.send(message);
                            producer.close();
                            sess.close();
                        }
                    }
                } catch (Exception e) {
                    logger.error("Unable to trigger master. Ex=" + e.getMessage(), e);
                }

            }
        }
        masterTimer = new Timer("Status Timer", true);
        masterTimer.scheduleAtFixedRate(new InnerMasterTask(), new Date(), 5000);
        //**********************************************************************
    }
    
    public void stopPubSub() {
        
        logger.info("Stopping PubSub");
        logger.info("Cancelling status timer...");
        statusTimer.cancel();
        logger.info("Cancelling master timer...");
        masterTimer.cancel();
        if (singletonconnection != null) {
            try {
                singletonconnection.stop();
            } catch (JMSException ex) {
                java.util.logging.Logger.getLogger(Dispatcher.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        if (persistentconnection != null) {
            try {
                persistentconnection.stop();
            } catch (JMSException ex) {
                java.util.logging.Logger.getLogger(Dispatcher.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        logger.info("PubSub stopped");
        
    }
    
    private String prepareBaseDirectoryForSingleton(){
        String directoryname = null;
        if (this.singletonBean.directory.endsWith(File.separator)) {
            directoryname = this.singletonBean.directory + "previous_versions";
        } else {
            directoryname = this.singletonBean.directory + File.separator + "previous_versions";
        }

        File theDir = new File(directoryname);

        for (String sing : this.singletonTopicsNoHistory.split(",")) {
            if (sing.length() > 0) {
                singletonsWithoutHistory.put(sing, sing);
            }
        }

        // if the directory does not exist, create it
        if (!theDir.exists()) {
            logger.info("creating directory: " + directoryname);
            boolean result = theDir.mkdirs();

            if (result) {
                logger.info("Directory:" + directoryname + " created");
            }
            else
                {
                    logger.error("Unable to create directory:" + theDir.getAbsolutePath() + " Error:"+result);
                }
        }
        
        return directoryname;
    }
    
    private void prepareDirectoryForSingleton(String sing, String directoryname) {
        if (sing.length() > 0) {
            String directname2 = directoryname + File.separator + sing;
            File theDir2 = new File(directname2);

            // if the directory does not exist, create it
            if (!theDir2.exists()) {
                logger.info("creating directory: " + directname2);
                boolean result = theDir2.mkdirs();                

                if (result) {
                    logger.info("Directory:" + directname2 + " created");
                }
                else
                {
                    logger.error("Unable to create directory:" + theDir2.getAbsolutePath() + " Error:"+result);
                }
            }
        }
    }

    public void createSingletonJMSConnection() {
        synchronized (this) {
            if (singletonconnection != null) {
                return;
            }
                    
            try {
                JmsComponent comp = (JmsComponent) CamelWorker.context.getBean("jms");
                JmsConfiguration conf = comp.getConfiguration();
                ConnectionFactory fact = conf.getConnectionFactory();
                logger.info("Creating singletonconnection");
                singletonconnection = fact.createConnection();
                singletonconnection.setExceptionListener(new ExceptionListener() {
                    public void onException(JMSException jmse) {
                        logger.error("[createSingletonJMSConnection] error occurs:  " + jmse.toString(), jmse);
                        try {
                            singletonsession.close();
                        } catch (Exception e) { 
                        }
                        try {
                            singletonconnection.close();
                        } catch (Exception e) {
                            
                        }
                        singletonsession = null;
                        singletonconnection = null;
                    }
                });

                singletonconnection.setClientID("PubSubSingletonWriter");
                singletonsession = singletonconnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                singletonBean.setSingletonsWithoutHistory(singletonsWithoutHistory);
                logger.info("Connected singletonconnection");
                this.singletonConsumers.clear();
                for (String topic : getSingletonTopics().split(",")) {
                    this.prepareSingletonTopic(topic);
                }

                singletonconnection.start();
            } catch (Exception e) {
                logger.error("Error. Ex=" + e.getMessage(), e);
                try {
                    singletonconnection.stop();
                } catch (Exception e2) {
                }
                singletonconnection = null;
            }
        }
    }
    
    private void prepareSingletonTopic(String topic) throws Exception {
        if (topic != null && topic.length() > 0) {
            if (!this.singletonConsumers.contains(topic)) {
                try {
                    logger.info("Adding singleton consumer:" + topic);
                    MessageConsumer consu = singletonsession.createDurableSubscriber(singletonsession.createTopic(topic), "SINGLETON-" + topic.replace("_", ""));
                    consu.setMessageListener(this.singletonBean);
                    singletonConsumers.add(topic);
                    logger.info("Added.");
                } catch (Exception e) {
                    logger.error("Unable to add " + topic + ", error is " + e.getMessage());
                }
            } else {
                logger.debug("singletonConsumers topic already exists:" + topic + ", do not adding it");
            }
        }
    }

    public void createPersistentJMSConnection() {
        synchronized (this) {
            if (persistentconnection != null) {
                return;
            }

            try {
                org.apache.camel.component.jms.JmsComponent comp = (org.apache.camel.component.jms.JmsComponent) CamelWorker.context.getBean("jms");
                JmsConfiguration conf = comp.getConfiguration();
                ConnectionFactory fact = conf.getConnectionFactory();
                logger.info("Creating persistentconnection");
                persistentconnection = fact.createConnection();
                persistentconnection.setExceptionListener(new ExceptionListener() {
                    public void onException(JMSException jmse) {
                        logger.error("[createPersistentJMSConnection] error occurs:  " + jmse.toString(), jmse);
                        try {
                            persistentproducer.close();
                        } catch (Exception e) { 
                        }
                        try {
                            persistentsession.close();
                        } catch (Exception e) { 
                        }
                        try {
                            persistentconnection.close();
                        } catch (Exception e) { 
                        }
                        persistentsession = null;
                        persistentproducer = null;
                        persistentconnection = null;
                        
                    }
                });
                persistentconnection.setClientID("PubSubPersistentWriter");

                persistentsession = persistentconnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                logger.info("Connected persistentconnection");
                this.getFileAppenderBean().listenersPerTopic.clear();
                for (String topic : getPersistentTopics().split(",")) {
                    this.preparePersistentTopic(topic);
                }
                persistentconnection.start();
                
            } catch (Exception e) {
                logger.info("Error. Ex=" + e.getMessage(), e);
                try {
                    persistentconnection.stop();
                } catch (Exception e2) {
                }
                persistentconnection = null;
            }
        }
    }
    
    private void preparePersistentTopic(String topic) throws Exception {
        
        if (topic != null && topic.length() > 0) {
            if (this.getFileAppenderBean().listenersPerTopic.containsKey(topic)) {
                logger.debug("Persistent topic already exists:" + topic + ", do not adding it");
            } else {
                try {
                    logger.info("Adding persistent consumer:" + topic);
                    MessageConsumer consu = persistentsession.createDurableSubscriber(persistentsession.createTopic(topic), "PERSISTENT-" + topic.replace("_", ""));
                    PersistentMessageListener mesli = new PersistentMessageListener(this.getFileAppenderBean(), topic);
                    this.getFileAppenderBean().listenersPerTopic.put(topic, mesli);

                    consu.setMessageListener(mesli);
                    logger.info("Added.");
                } catch (Exception e) {
                    logger.error("Unable to add " + topic + ", error is " + e.getMessage());
                }
            }

        }
    }

    public void createReplicationJMSConnection() {
        synchronized (this) {
            if (replicationconnection != null) {
                return;
            }
            try {
                org.apache.camel.component.jms.JmsComponent comp = (org.apache.camel.component.jms.JmsComponent) CamelWorker.context.getBean("jms");
                JmsConfiguration conf = comp.getConfiguration();
                ConnectionFactory fact = conf.getConnectionFactory();
                logger.info("Creating replicationconnection");
                replicationconnection = fact.createConnection();
                replicationconnection.setClientID("PubSubreplicationWriter");
                replicationconnection.setExceptionListener(new ExceptionListener() {
                    public void onException(JMSException jmse) {
                        logger.error("[createReplicationJMSConnection] error occurs:  " + jmse.toString(), jmse);
                        try {
                            replicationconnection.close();
                        } catch (Exception e) { 
                        }
                        try {
                            replicationsession.close();
                        } catch (Exception e) { 
                        }
                        replicationconnection = null;
                        replicationsession = null;
                    }
                });
                replicationsession = replicationconnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                logger.info("Connected replicationconnection");
                
                replicationListenersPerTopic.clear();
                for (String topic : (singletonTopics + ',' + standardTopics + "," + persistentTopics).split(",")) {
                    this.prepareReplicationTopic(topic);
                }
                replicationconnection.start();
            } catch (Exception e) {
                logger.info("Error. Ex=" + e.getMessage(), e);
                try {
                    replicationconnection.stop();
                } catch (Exception e2) {
                }
                replicationconnection = null;
            }
        }
    }
    
    private void prepareReplicationTopic(String topic) throws Exception {
        if (topic != null && topic.length() > 0) {
            
            if (replicationListenersPerTopic.containsKey(topic)) {
                logger.debug("Replication topic already exists:" + topic);
            } else {
                try {
                    logger.info("Adding replication consumer:" + topic);
                    MessageConsumer consu = null;
                    if (!isStandard(topic)) {
                        consu = replicationsession.createDurableSubscriber(replicationsession.createTopic(topic), "REPLICATION-" + topic.replace("_", ""));
                    } else {
                        consu = replicationsession.createConsumer(replicationsession.createTopic(topic));
                    }

                    ReplicationMessageListener mesli = new ReplicationMessageListener(this, topic);
                    replicationListenersPerTopic.put(topic, mesli);
                    consu.setMessageListener(mesli);
                    logger.info("Added.");
                } catch (Exception e) {
                    logger.error("Unable to add " + topic + ", error is " + e.getMessage());
                }
            }
        }
    }

    public void createJMSConnection() {
        synchronized (this) {
            if (connection != null) {
                return;
            }
            try {
                JmsComponent comp = (JmsComponent) CamelWorker.context.getBean("jms");
                JmsConfiguration conf = comp.getConfiguration();
                ConnectionFactory fact = conf.getConnectionFactory();
                logger.info("Creating JMS connection factory");
                connection = fact.createConnection();
                
                connection.setClientID("PubSubSingleton");
                connection.setExceptionListener(new ExceptionListener() {
                    public void onException(JMSException jmse) {
                        logger.error("[createJMSConnection] error occurs  " + jmse.toString(), jmse);
                        try {
                            connection.close();
                        } catch (Exception e) { 
                        }
                        connection = null;
                    }
                });
                connection.start();
                logger.info("Connected connection");
            } catch (Exception e) {
                logger.info("Error. Ex=" + e.getMessage(), e);
                try {
                    connection.stop();
                } catch (Exception e2) {
                }
                connection = null;
            }
        }
    }

    @Override
    public void process(Exchange exchng) throws Exception {
        String res = exchng.getFromEndpoint().getEndpointUri();
        String[] split = res.split(":");
        String topic = split[split.length - 1].split("\\?")[0];
        String resc = (String) exchng.getIn().getBody();


        if (topic.compareTo("PUBSUB_CONTROL") == 0) {
            logger.info("PubSub Control message received:" + resc);

            Properties properties = new Properties();
            properties.load(new StringReader(resc));

            String Action = properties.getProperty("Action");
            if (Action != null) {

                if (connection == null) {
                    createJMSConnection();
                }
                if (connection == null) {
                    logger.error("Unable to create connection.");
                    return;
                }
                String resynchqueue = properties.getProperty("ResynchQueue");

                if (Action.compareTo("AskServerID") == 0) {
                    Session sess = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    Destination destination = sess.createQueue(resynchqueue);
                    MessageProducer producer = sess.createProducer(destination);
                    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                    TextMessage message;

                    message = sess.createTextMessage("Action=SetServerID");
                    message.setIntProperty("ServerID", serverID);
                    message.setBooleanProperty("IsMaster", masterHandlerBean.Master);
                    producer.send(message);
                    producer.close();
                    sess.close();
                    return;
                }

                String topics = properties.getProperty("Topics");
                String[] currenttime = properties.getProperty("CurrentTime").split(",");
                Map<String, ResynchTopic> topics2resynch = new HashMap<String, ResynchTopic>();

                for (String onetopicdes : topics.split(",")) {
                    if (onetopicdes.length() > 0) {
                        String[] des = onetopicdes.split(";");
                        logger.info("Topic:" + des[0]);
                        if (isSingleton(des[0])) {
                            logger.info("Handling singleton.");

                            Session sess = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                            Destination destination = sess.createQueue(resynchqueue);
                            MessageProducer producer = sess.createProducer(destination);
                            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                            TextMessage message;

                            try {
                                String resstr = singletonBean.readFileAsString(singletonBean.getDirectory() + des[0] + ".txt");
                                message = sess.createTextMessage("Topic=" + des[0] + "\r\n" + resstr);

                                message.setLongProperty("PubSubIDUTC", guidGenerator.nextUniqueID());
                                producer.send(message);

                                logger.info("Message sent.");
                            } catch (Exception e) {
                                logger.info("Singleton " + des[0] + " not found. Ex=" + e.getMessage());

                            }
                            message = sess.createTextMessage("Action=EndOfResync\r\nTopic=" + des[0]);
                            message.setLongProperty("PubSubID", serverID);
                            producer.send(message);
                            producer.close();
                            sess.close();

                        } else {
                            if (isPersistent(des[0])) {
                                if (topics2resynch.containsKey(des[0])) {
                                    logger.info("Key already asked:" + des[0]);
                                } else {
                                    ResynchTopic top = new ResynchTopic(des);
                                    topics2resynch.put(des[0], top);
                                }

                            } else if (isStandard(des[0])) {
                                logger.info("Handling singleton.");

                                Session sess = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                                Destination destination = sess.createQueue(resynchqueue);
                                MessageProducer producer = sess.createProducer(destination);
                                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                                TextMessage message;

                                message = sess.createTextMessage("Action=EndOfResync\r\nTopic=" + des[0]);
                                message.setLongProperty("PubSubID", serverID);
                                producer.send(message);
                                producer.close();
                                sess.close();
                            } else {
                                logger.error("Unknown topic:" + des[0]);
                            }
                        }
                    }
                }
                if (topics2resynch.size() > 0) {

                    ResynchClientThread inthread = new ResynchClientThread(topics2resynch, Long.parseLong(currenttime[0]), currenttime[1], resynchqueue, this);
                    inthread.start();

                }

            }
        }
    }

    public boolean isSingleton(String aTopic) {
        return ("," + singletonTopics + ",").contains("," + aTopic + ",");

    }

    public boolean isStandard(String aTopic) {
        return ("," + standardTopics + ",").contains("," + aTopic + ",");

    }

    public boolean isPersistent(String aTopic) {
        return ("," + persistentTopics + ",").contains("," + aTopic + ",");

    }

    public void resynchClient(Map<String, ResynchTopic> aResynchTopicList, long anEndID, String anInEndTime, String aResynchQueue) throws JMSException {
        int numberofmessages = 0;
        SimpleDateFormat format = new SimpleDateFormat("ddMMMyyyy HH:mm:ss");
        SimpleDateFormat format2 = new SimpleDateFormat("ddMMyyyyHHmmssS");

        

        Date anEndTime;
        try {
            anEndTime = format2.parse(anInEndTime);
        } catch (ParseException ex) {
            logger.error("Unable to parse end time. Ex=" + ex.getMessage(), ex);
            return;
        }

        Calendar cal = Calendar.getInstance();
        cal.set(2900, 1, 1);

        Date starttime = cal.getTime();

        Session sess = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = sess.createQueue(aResynchQueue);
        MessageProducer producer = sess.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        for (ResynchTopic top : aResynchTopicList.values()) {
            if (top.startDate.before(starttime)) {
                starttime = top.startDate;
            }
        }

        logger.info("Resynch from:" + format.format(starttime) + " to:" + format.format(anEndTime));
        if (anEndTime.after(starttime)) {
            TextMessage message;
            while (anEndTime.after(starttime)) {

                String filename = fileAppenderBean.getFileForDate(starttime);

                logger.info("Processing file:" + filename);

                cal.setTime(starttime); // sets calendar time/date
                cal.add(Calendar.HOUR_OF_DAY, 1); // adds one hour
                starttime = cal.getTime();

                File file = new File(filename);
                List<OneMessage> mes2send = new ArrayList<OneMessage>();
                if (file.exists()) {
                    BufferedReader br = null;
                    try {
                        br = new BufferedReader(new FileReader(file));
                        logger.info("File opened");
                        String line;
                        while ((line = br.readLine()) != null) {
                            if (!line.startsWith("NA")) {
                                //logger.info("File line:"+line);
                                try {
                                    int ind1 = line.indexOf("\t");
                                    int ind2 = line.indexOf("\t", ind1 + 1);
                                    int ind3 = line.indexOf("\t", ind2 + 1);
                                    String field1 = line.substring(0, ind1);
                                    String field2 = line.substring(ind1 + 1, ind2);
                                    String field3 = line.substring(ind2 + 1, ind3);
                                    String field4 = line.substring(ind3 + 1);
                                    if (aResynchTopicList.containsKey(field3)) {
                                        ResynchTopic top = aResynchTopicList.get(field3);
                                        long curpos = Long.parseLong(field1);
                                        if ((top.pubSubID <= curpos)
                                                && (curpos <= anEndID)) {
                                            OneMessage mymess = new OneMessage(field3, field4, Long.parseLong(field1));
                                            //logger.info("MesToSend:"+field3+" Mes:"+field4+" PubSub ID:"+Long.parseLong(field1));
                                            mes2send.add(mymess);

                                        }
                                    }
                                } catch (Exception e) {
                                    logger.error("Unable to read line:" + line + " Ex=" + e.getMessage());
                                }
                            }
                        }
                        br.close();
                    } catch (Exception ex) {
                        logger.error("Error while reading file. Ex=" + ex.getMessage());
                    }

                }
                logger.info("Preparing " + mes2send.size() + " messages.");
                int packetsize = 20000;
                try {
                    Collections.sort(mes2send);
                } catch (Exception e) {
                    logger.error("Unable to sort. Ex=" + e.getMessage(), e);
                }
                /*for(OneMessage aaames:mes2send)
                 {
                 logger.info("MesToSend:"+aaames.topic+" Mes:"+aaames.message+" PubSub ID:"+aaames.pubSubID); 
                 }*/
                StringBuilder build = new StringBuilder();

                logger.info("Sending " + mes2send.size() + " messages.");

                for (OneMessage mess : mes2send) {
                    build.append("MultiTopic=" + mess.topic + ",PubSubID=" + mess.pubSubID + "\r\n" + mess.message + ">NeXtMes>\r\n");
                    numberofmessages++;
                    if (build.length() > packetsize) {
                        try {
                            message = sess.createTextMessage(build.toString());
                            //message = sess.createTextMessage("Topic="+mess.topic+"\r\n"+mess.message);
                            //message.setLongProperty("PubSubID",mess.pubSubID);
                            producer.send(message);

                            logger.info("Message #" + numberofmessages + " sent");
                        } catch (Exception e) {
                            logger.error("Unable to send message. Ex=" + e.getMessage());
                        }
                        build = new StringBuilder();
                    }
                }
                if (build.length() > 0) {
                    try {
                        message = sess.createTextMessage(build.toString());
//                        message = sess.createTextMessage("Topic="+mess.topic+"\r\n"+mess.message);
                        //message.setLongProperty("PubSubID",mess.pubSubID);
                        producer.send(message);

                        //logger.info("Message sent:"+message);
                    } catch (Exception e) {
                        logger.error("Unable to send message. Ex=" + e.getMessage());
                    }
                }
                mes2send.clear();
            }

            for (ResynchTopic top : aResynchTopicList.values()) {
                message = sess.createTextMessage("Action=EndOfResync\r\nTopic=" + top.topic);
                message.setLongProperty("PubSubID", serverID);
                producer.send(message);
            }
            logger.info("Messages resynch:" + numberofmessages);
            numberOfMessagesResynched += numberofmessages;
        } else {
            logger.error("Start time after end time in resynch.");
        }

        producer.close();
        sess.close();
    }

    void onMessageForReplication(Message msg, String topic) {
        //logger.info("Must add message for replication:"+topic);
        String MessageAsText="";
        
        try
        {
            if(msg instanceof BytesMessage)
            {
                BytesMessage byteMessage=(BytesMessage)msg;
                byte[] byteArr = new byte[(int)byteMessage.getBodyLength()];
                byteMessage.readBytes(byteArr); 
                MessageAsText = new String(byteArr);//, "UTF-16");  
            }
            else if(msg instanceof TextMessage)
            {
                MessageAsText=((TextMessage)msg).getText();
            }
            else if (!(msg instanceof TextMessage)) {
                return;
            }


            try {
                if (msg.propertyExists("IsResynch")) {
                    return;
                }

                String suffix = "";

                if (isPersistent(topic)) {
                    suffix = "_PERSISTENT";
                } else {
                    if (isSingleton(topic)) {
                        suffix = "_SINGLETON";
                    }
                }

                Session sess = replicationconnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                //Session sess=replicationsession;
                TextMessage newmes = sess.createTextMessage();
                newmes.setStringProperty("IsResynch", "1");
                newmes.setText(MessageAsText);
                newmes.setStringProperty("Topic", topic);

                /*if (msg.getStringProperty("PubSubID") != null)
                 {
                 newmes.setStringProperty("PubSubID", msg.getStringProperty("PubSubID"));
                 }
                 if (msg.getStringProperty("PubSubIDUTC") != null)
                 {
                 newmes.setStringProperty("PubSubIDUTC", msg.getStringProperty("PubSubIDUTC"));
                 }
                 if (msg.getStringProperty("ClientName") != null)
                 {
                 newmes.setStringProperty("ClientName", msg.getStringProperty("ClientName"));
                 }*/
                try {
                    newmes.setLongProperty("OriginalJMSTimestamp", msg.getJMSTimestamp());
                } catch (Exception e) {
                }

                try {
                    newmes.setLongProperty("JMSTimestamp", msg.getLongProperty("JMSTimestamp"));
                } catch (Exception e) {
                }

                Enumeration<String> en = msg.getPropertyNames();

                while (en.hasMoreElements()) {
                    String value = en.nextElement();
                    try {
                        if (newmes.getObjectProperty(value) == null) {
                            Object obj = msg.getObjectProperty(value);
                            newmes.setObjectProperty(value, obj);
                            //logger.info("Set Header:"+value+" to:"+obj);
                        }
                    } catch (Exception e) {
                        logger.error("Unable to set header:" + value);
                    }

                }

                for (String server : otherServers.split(",")) {

                    if (server.length() > 0) {
                        Destination destination = sess.createQueue("REPLICATION" + suffix + "_TOSRV" + server);
                        MessageProducer producer = sess.createProducer(destination);
                        if (isStandard(topic)) {
                            producer.setTimeToLive(10000);
                        }
                        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                        producer.send(newmes);
                        producer.close();
                    }
                }
                sess.close();

            } catch (Exception e) {
                logger.error("Unable to send resynch message. Ex=" + e.getMessage());
            }
        }
        catch(Exception e2)
        {
            logger.error("Unable to send resynch message. Ex=" + e2.getMessage());
        }
    }

    void persistentMessageReceived(Message msgin, String topic) throws JMSException {
        String MessageAsText="";
        try 
        {
            if(msgin instanceof BytesMessage)
            {
                BytesMessage byteMessage=(BytesMessage)msgin;
                byte[] byteArr = new byte[(int)byteMessage.getBodyLength()];
                byteMessage.readBytes(byteArr); 
                MessageAsText = new String(byteArr);//, "UTF-16");  
            }
            else if(msgin instanceof TextMessage)
            {
                MessageAsText=((TextMessage)msgin).getText();
            }
            else if (!(msgin instanceof TextMessage)) {
                return;
            }


            Message msg = msgin;

            if (persistentMessageQueue.length() > 0) {
                if (persistentsession != null) {
                    if (persistentproducer == null) {
                        Destination destination = persistentsession.createQueue(persistentMessageQueue);
                        persistentproducer = persistentsession.createProducer(destination);

                    }
                    if (persistentproducer != null) {
                        TextMessage mes = persistentsession.createTextMessage();

                        if (msg.getStringProperty("ClientName") == null) {
                            mes.setStringProperty("ClientName", "NA");
                        } else {
                            mes.setStringProperty("ClientName", msg.getStringProperty("ClientName"));
                        }

                        mes.setStringProperty("Topic", topic);

                        try {
                            mes.setLongProperty("PubSubID", msg.getLongProperty("PubSubID"));
                        } catch (Exception e) {
                            mes.setLongProperty("PubSubID", this.serverID);
                        }

                        java.lang.Long timestamp = null;
                        try {
                            timestamp = msg.getLongProperty("OriginalJMSTimestamp");
                        } catch (Exception e) {
                        }

                        if (timestamp == null) {
                            timestamp = msg.getLongProperty("JMSTimestamp");
                        }
                        Date date = new Date(timestamp.longValue());
                        mes.setStringProperty("SQLDate", sqldateformatter.format(date));
                        mes.setText(MessageAsText.replaceAll("\r", "[(CR)]").replaceAll("\n", "[(LF)]"));
                        persistentproducer.send(mes);
                    }
                }
            }
        } 
        catch (JMSException e) {
            logger.error("JMSException occurred while processing the message", e);
            java.util.logging.Logger.getLogger(Dispatcher.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    public String validateSleutel(String aNewSleutel) {
        return "Success";        
    }
    
    public JSONObject computeTopicsToAdd() {
        
        try {
            
            TopicsConfiguration conf = CamelWorker.fetchTopicsFromConfigFile();
            String newSingletons = CamelWorker.cleanSubscriptionString(conf.singletonTopics, null);
            String newstandardTopics = CamelWorker.cleanSubscriptionString(conf.standardTopics, null);
            String newpersistentTopics = CamelWorker.cleanSubscriptionString(conf.persistentTopics, null);
            
            JSONArray newSingleton = new JSONArray();
            JSONArray deletedSingleton = new JSONArray();
            List<String> singletonsList = Arrays.asList(getSingletonTopics().split(","));
            List<String> newSingletonsList = Arrays.asList(newSingletons.split(","));
            for (String key : newSingletonsList) {
                if (!singletonsList.contains(key)) {
                    newSingleton.add(key);        
                }
            }
            for (String key : singletonsList) {
                if (!newSingletonsList.contains(key)) {
                    deletedSingleton.add(key);        
                }
            }
            
            JSONArray newStandart = new JSONArray();
            JSONArray deletedStandart = new JSONArray();
            List<String> standartList = Arrays.asList(getStandardTopics().split(","));
            List<String> newStandartList = Arrays.asList(newstandardTopics.split(","));
            for (String key : newStandartList) {
                if (!standartList.contains(key)) {
                    newStandart.add(key);     
                }
            }
            for (String key : standartList) {
                if (!newStandartList.contains(key)) {
                    deletedStandart.add(key);     
                }
            }
            
            JSONArray newPersistent = new JSONArray();
            JSONArray deletedPersistent = new JSONArray();
            List<String> persistentList = Arrays.asList(getPersistentTopics().split(","));
            List<String> newPersistentList = Arrays.asList(newpersistentTopics.split(","));
            for (String key : newPersistentList) {
                if (!persistentList.contains(key)) {
                    newPersistent.add(key);    
                }
            }
            for (String key : persistentList) {
                if (!newPersistentList.contains(key)) {
                    deletedPersistent.add(key);    
                }
            }

            JSONObject obj = new JSONObject();
            obj.put("singletonsToAdd", newSingleton);
            obj.put("singletonsToDelete", deletedSingleton);
            obj.put("standartToAdd", newStandart);
            obj.put("standartToDelete", deletedStandart);
            obj.put("persistentToAdd", newPersistent);
            obj.put("persistentToDelete", deletedPersistent);
            
            return obj;
        } catch (Exception e) {
            logger.error("Error computing topics to add, error is ", e);
            return null;
        }
        
    }
    
    public void applyNewConfiguration() throws Exception {
        
        TopicsConfiguration topics = CamelWorker.fetchTopicsFromConfigFile();
        if (topics == null){
            throw new IllegalStateException("Unable to read properties file");
        }

        Set<String> alltopics = new HashSet<String>();
        topics.standardTopics = CamelWorker.cleanSubscriptionString(this.getStandardTopics() + "," + topics.standardTopics, alltopics);
        topics.persistentTopics = CamelWorker.cleanSubscriptionString(this.getPersistentTopics()+ "," + topics.persistentTopics, alltopics);
        topics.singletonTopics = CamelWorker.cleanSubscriptionString(this.getSingletonTopics()+ "," + topics.singletonTopics, alltopics);
        topics.singletonTopicsNoHistory = CamelWorker.cleanSubscriptionString(this.getSingletonTopicsNoHistory() + "," + topics.singletonTopicsNoHistory, null);
        

        this.setStandardTopics(topics.standardTopics);
        this.setSingletonTopics(topics.singletonTopics);
        this.setSingletonTopicsNoHistory( topics.singletonTopicsNoHistory);
        this.setPersistentTopics(topics.persistentTopics);
        
        
        // We update replication
        for (String topic : (singletonTopics + ',' + standardTopics + "," + persistentTopics).split(",")) {
            this.prepareReplicationTopic(topic);
        }

        // We update the singletons handlings
        String directoryname = this.prepareBaseDirectoryForSingleton();
        for (String topic : getSingletonTopics().split(",")) {
            this.prepareSingletonTopic(topic);
            this.prepareDirectoryForSingleton(topic, directoryname);
        }

        // We update the persistence handling
        for (String topic : getPersistentTopics().split(",")) {
            this.preparePersistentTopic(topic);
        }
        
    }

    public String getConfigurationAsJson() {
        JSONObject mainobj = new JSONObject();

        JSONArray list = new JSONArray();
        mainobj.put("singletons", list);

        for (String key : this.singletonTopics.split(",")) {
            JSONObject cliobj = new JSONObject();
            cliobj.put("topic", key);
            list.add(cliobj);
        }

        list = new JSONArray();
        mainobj.put("persistents", list);

        for (String key : this.persistentTopics.split(",")) {
            JSONObject cliobj = new JSONObject();
            cliobj.put("topic", key);
            list.add(cliobj);
        }

        list = new JSONArray();
        mainobj.put("regulars", list);

        for (String key : this.standardTopics.split(",")) {
            JSONObject cliobj = new JSONObject();
            cliobj.put("topic", key);
            list.add(cliobj);
        }
        
        mainobj.put("update", computeTopicsToAdd());
        return mainobj.toJSONString();
    }

    public void sendStartNotification() {
        try {
            if (connection != null) {
                logger.info("Wait 5 seconds to send ready notification.");
                Thread.sleep(5000);
                logger.info("Send ready signal.");
                Session sess = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Destination destination = sess.createTopic("COMMAND");
                MessageProducer producer = sess.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                TextMessage message;

                message = sess.createTextMessage("Kernel/" + serverID + "/STARTNOTIFICATION/");
                message.setLongProperty("PubSubID", serverID);
                producer.send(message);
                producer.close();
                sess.close();
            }
        } catch (Exception e) {
            logger.error("Unable to send start notification. Ex=" + e.getMessage(), e);
        }
    }
    
    public void sendUpdateNotification() {
        try {
            if (connection != null) {
                Session sess = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Destination destination = sess.createTopic("COMMAND");
                MessageProducer producer = sess.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                TextMessage message;

                message = sess.createTextMessage("Kernel/" + serverID + "/UPDATENOTIFICATION/");
                message.setLongProperty("PubSubID", serverID);
                producer.send(message);
                producer.close();
                sess.close();
            }
        } catch (Exception e) {
            logger.error("Unable to send start notification. Ex=" + e.getMessage(), e);
        }
    }
}
