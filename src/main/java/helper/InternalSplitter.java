package helper;

import camelworker.CamelWorker;
import static helper.ExcelConverter.logger;
import java.io.Serializable;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.component.jms.JmsConfiguration;
import org.apache.activemq.ActiveMQConnectionFactory; // Utilisation de la classe ActiveMQConnectionFactory pour Jakarta
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

public class InternalSplitter implements Processor {

    static final Logger logger = LoggerFactory.getLogger("InternalSplitter");

    Connection connection = null;
    Session sess = null;
    HashMap<String, MessageProducer> producers = new HashMap<String, MessageProducer>();

    org.apache.camel.component.jms.JmsComponent jmsRef;
    String connectionName;

    public String getConnectionName() {
        return connectionName;
    }

    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
    }

    public JmsComponent getJmsRef() {
        return jmsRef;
    }

    public void setJmsRef(JmsComponent jmsRef) {
        this.jmsRef = jmsRef;
    }

    public void process(Exchange exchng) throws Exception {

        if (connection == null) {
            try {
                // Récupération de la configuration de JMS dans Camel
                JmsConfiguration conf = jmsRef.getConfiguration();

                // Utilisation de ActiveMQConnectionFactory avec Jakarta JMS
                ActiveMQConnectionFactory fact = new ActiveMQConnectionFactory("tcp://localhost:61616"); // Remplacez par l'URL correcte de votre broker
                connection = (Connection) fact.createConnection();
                connection.start();
                sess = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            } catch (JMSException e) {
                logger.info("Error. Ex=" + e.getMessage(), e);
                try {
                    if (connection != null) {
                        connection.stop();
                    }
                } catch (JMSException e2) {
                }
                sess = null;
                connection = null;
            }
        }

        if (connection == null) {
            logger.error("Unable to create connection.");
            return;
        }

        String destination = (String) exchng.getIn().getHeaders().get("jmsDestination");

        if (destination == null) {
            logger.error("jmsDestination not specified");
        } else {
            MessageProducer prod;

            synchronized (producers) {
                // Si le producteur pour la destination existe déjà, on l'utilise
                if (producers.containsKey(destination)) {
                    prod = producers.get(destination);
                } else {
                    // Création d'une nouvelle destination et d'un producteur
                    Destination dest = sess.createQueue(destination); // Vous pouvez aussi utiliser createTopic() pour les topics
                    prod = sess.createProducer(dest);
                    producers.put(destination, prod);
                }
            }

            org.apache.camel.Message inmes = exchng.getIn();
            int index = 0;
            // Envoi des messages un par un
            for (Object obj : (List<Object>) exchng.getIn().getBody()) {
                ObjectMessage testm = sess.createObjectMessage();

                testm.setIntProperty("CamelSplitIndex", index);

                // Copie des headers du message Camel dans le message JMS
                for (Map.Entry<String, Object> entry : inmes.getHeaders().entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue();

                    try {
                        if (testm.getObjectProperty(key) == null) {
                            testm.setObjectProperty(key, value);
                            //logger.info("Set Header:"+value+" to:"+obj);
                        }
                    } catch (JMSException e) {
                        logger.error("Unable to set header:" + value);
                    }
                }

                testm.setObject((Serializable) obj); // Sérialisation de l'objet pour l'envoi via JMS
                prod.send(testm); // Envoi du message
                index++;
            }
        }
    }
}
