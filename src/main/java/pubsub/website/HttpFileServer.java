package pubsub.website;

import camelworker.CamelWorker;
import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Destination;
import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.camel.component.jms.JmsConfiguration;
import org.apache.camel.component.jms.JmsComponent;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pubsub.Dispatcher;
import pubsub.jmx.Supervisor;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;

/**
 * @author Arnaud Marchand
 */
public class HttpFileServer implements MessageListener {

    static final Logger logger = LoggerFactory.getLogger("HttpFileServer");

    private long totalmessages = 0;
    private int port;
    private Dispatcher pubSubDispatcher;
    private Supervisor jmxSupervisor;

    private Connection connection = null;
    private Session session = null;
    private final ArrayList<OneSecondRecords> records = new ArrayList<OneSecondRecords>();
    
    private final List<String> alreadyAddedConsumers = new ArrayList<String>();
    
    private static Timer tm = null;

    public HttpFileServer() {
        this.checkJMSConnection();
        initTimer();
    }

    public HttpFileServer(int port) {
        this.port = port;
        this.checkJMSConnection();
        initTimer();
    }
    
    private synchronized void initTimer(){
        if (tm == null) {
            tm = new Timer();
            tm.scheduleAtFixedRate(new TimerTask(){
                @Override
                public void run() {
                    checkJMSConnection();
                }
            }, 1000, 60000);
        }
    }
    
    public Supervisor getJmxSupervisor() {
        return jmxSupervisor;
    }

    public void setJmxSupervisor(Supervisor jmxSupervisor) {
        this.jmxSupervisor = jmxSupervisor;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
    
    public Dispatcher getPubSubDispatcher() {
        return pubSubDispatcher;
    }

    public void setPubSubDispatcher(Dispatcher pubSubDispatcher) {
        this.pubSubDispatcher = pubSubDispatcher;
        HttpFileServerHandler.pubSubDispatcher = this.pubSubDispatcher;
        HttpFileServerHandler.httpFileServer = this;
    }
    
    public void checkJMSConnection() {
        synchronized (this) {
            if (connection != null) {
                return;
            }

            if (CamelWorker.context == null) {
                return;
            }
            try {
                JmsComponent comp = (JmsComponent) CamelWorker.context.getBean("jms");
                JmsConfiguration conf = comp.getConfiguration();
                ConnectionFactory fact = (ConnectionFactory) conf.getConnectionFactory();
                connection = fact.createConnection();
                connection.setExceptionListener(new ExceptionListener() {
                    public void onException(JMSException jmse) {
                        logger.error("[checkJMSConnection] error occurs  " + jmse.toString(), jmse);
                        try {
                            connection.close();
                        } catch (Exception e) { 
                        }
                        connection = null; 
                    }
                });
                connection.setClientID("Visualizer");

                this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                this.alreadyAddedConsumers.clear();
                this.prepareVisualizerListener();

                connection.start();
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

    public void prepareVisualizerListener() throws Exception {
        if (this.session == null) {
            return;
        }
        
        for (String topic : (pubSubDispatcher.getSingletonTopics()
                + ',' + pubSubDispatcher.getStandardTopics() + ","
                + pubSubDispatcher.getPersistentTopics()).split(",")) {
            
            if (topic != null && topic.length() > 0 && !alreadyAddedConsumers.contains(topic)) {
                logger.debug("Adding consumer:" + topic);
                MessageConsumer consu = session.createConsumer(session.createTopic(topic));
                consu.setMessageListener(this);
                alreadyAddedConsumers.add(topic);
            } else {
                logger.debug("consumer topic already exists:" + topic + ", do not adding it");
            }
        }
    }

    public void run() {
        ServerBootstrap bootstrap = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));

        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory(new HttpFileServerPipelineFactory());

        // Bind and start to accept incoming connections.
        bootstrap.bind(new InetSocketAddress(port));
    }

    public static void main(String[] args) {
        int port;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        } else {
            port = 8087;
        }
        new HttpFileServer(port).run();
    }

    public String getJSONString(long anID) {

        long limitsecond = (new Date().getTime() / 1000) - 1;
        JSONObject mainobj = new JSONObject();

        JSONArray list = new JSONArray();
        mainobj.put("Data", list);
        mainobj.put("CurrentSecond", limitsecond);
        mainobj.put("TotalMessages", totalmessages);

        synchronized (records) {
            for (OneSecondRecords recs : records) {
                if (recs.CurrentSecond <= anID) {
                    continue;
                }
                if (recs.CurrentSecond >= limitsecond) {
                    continue;
                }
                JSONObject obj = new JSONObject();
                obj.put("CurrentSecond", recs.CurrentSecond);
                JSONArray topiclist = new JSONArray();
                obj.put("Topics", topiclist);

                for (Entry<String, List<OneMessage>> mespertopic : recs.Records.entrySet()) {
                    JSONObject objpersec = new JSONObject();
                    objpersec.put("Topic", mespertopic.getKey());
                    objpersec.put("Count", mespertopic.getValue().size());
                    JSONArray meslist = new JSONArray();
                    objpersec.put("Messages", meslist);
                    for (OneMessage mes : mespertopic.getValue()) {
                        JSONObject onemes = new JSONObject();
                        onemes.put("Message", mes.Message);
                        onemes.put("PubSubID", mes.PubSubID);
                        if (mes.ClientName.length() > 0) {
                            onemes.put("ClientName", mes.ClientName);
                        }
                        meslist.add(onemes);
                    }

                    topiclist.add(objpersec);
                }

                list.add(obj);
            }
        }
        return mainobj.toJSONString();
    }

    @Override
    public void onMessage(Message msg) {
        totalmessages++;
        String MessageAsText = "";

        try {
            if (msg instanceof BytesMessage) {
                BytesMessage byteMessage = (BytesMessage) msg;
                byte[] byteArr = new byte[(int) byteMessage.getBodyLength()];
                byteMessage.readBytes(byteArr);
                MessageAsText = new String(byteArr);
            } else if (msg instanceof TextMessage) {
                MessageAsText = ((TextMessage) msg).getText();
            } else if (!(msg instanceof TextMessage)) {
                return;
            }
        } catch (Exception e) {
            logger.error("Unable to read message. Ex=" + e.getMessage(), e);
        }

        Message mes = msg;
        try {
            synchronized (records) {

                Destination des = mes.getJMSDestination();
                String destination = des.toString().replace("topic://", "");

                long cursecond = new Date().getTime() / 1000;
                OneSecondRecords curr;
                if (records.size() == 0) {
                    curr = new OneSecondRecords(cursecond);
                    records.add(curr);
                } else {
                    if ((records.get(records.size() - 1)).CurrentSecond == cursecond) {
                        curr = records.get(records.size() - 1);
                    } else {
                        curr = new OneSecondRecords(cursecond);
                        records.add(curr);
                    }
                }
                String pubsubid = mes.getStringProperty("PubSubID");
                if (pubsubid == null) {
                    pubsubid = mes.getStringProperty("PubSubID");
                }

                // Appel de la méthode AddOneValue pour ajouter un message
                curr.AddOneValue(destination, pubsubid, mes.getStringProperty("ClientName"), MessageAsText);

                // Limitation de la taille de la liste records
                if (records.size() > 180) {
                    records.remove(0);
                }
            }
        } catch (Exception ex) {
            logger.error("Unable to add record. Ex=" + ex.getMessage(), ex);
        }
    }
}
