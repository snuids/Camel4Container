///Users/snuids/ICTCS/APSB/Configs
////Users/snuids/Documents/GitHub/Camel4Container/data/camel-context.xml
///
package camelworker;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.*;
import java.util.Map.Entry;
import java.io.*;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.equans.camel.component.pubsub.PubSubComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CamelWorker {
    static final Logger logger = LoggerFactory.getLogger("CamelWorker");
    
    public static final String version = "2.0.0";
    public static String camelVersion = "NA";    
    public static ApplicationContext context;
    public static void main(String[] args) {
        if (args.length < 1) {
            logger.info("Usage: java -jar <jarfile> path/to/camel-context.xml");
            System.exit(1);
        }

        logger.info("SJDKSKJDKSJDK $$$$$$$$$$$$$$$$$$$$$$$$$");

        Map<String, String> env = System.getenv();
        for (Map.Entry<String, String> entry : env.entrySet()) {
            logger.info(entry.getKey() + ": " + entry.getValue());
        }

        String path = "file:" + args[0];
        Path filePath = Paths.get(args[0]);

        // Load the Spring context from the external XML file
        ClassPathXmlApplicationContext applicationContext
                = new ClassPathXmlApplicationContext(path);

        context = applicationContext;
        // Get the Camel context from the Spring context
        CamelContext camelContext = applicationContext.getBean(CamelContext.class);
        camelVersion = camelContext.getVersion();
        org.apache.camel.component.activemq.ActiveMQComponent amq = applicationContext.getBean(org.apache.camel.component.activemq.ActiveMQComponent.class);

        try {
            logger.info("Setting PubSub Camel Version...");

            // Obtenez le CamelContext
            //CamelContext camelContext = (CamelContext) context.getBean("camel");

            // Récupérer le PubSubComponent à partir du CamelContext
            PubSubComponent pubSubComponent = (PubSubComponent) camelContext.getComponent("pubsub");

            // Vérifiez si le composant est nul
            if (pubSubComponent != null) {
                // Configurez la version Camel
                pubSubComponent.setCamelVersion(CamelWorker.version);
                logger.info("Successfully set the Camel version for PubSubComponent.");
            } else {
                logger.info("PubSubComponent not found in Camel context.");
            }
        } catch (Exception e) {
            logger.info("Error while setting PubSub Camel version.");
        }

        
        try {
            // Start the Camel context
            camelContext.start();
            // Monitor the file for changes
            WatchService watchService = FileSystems.getDefault().newWatchService();
            filePath.getParent().register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
            logger.info("Camel context started...");
            MBeanServer mBeanServer = camelContext.getManagementStrategy().getManagementAgent().getMBeanServer();
            while (true) {

                if ((mBeanServer != null) && (amq != null)) {
                    computeRouteStatus(mBeanServer, amq);
                }

                WatchKey key = watchService.poll(5, TimeUnit.SECONDS);
                if (key != null) {
                    for (WatchEvent<?> event : key.pollEvents()) {
                        if (event.kind() == StandardWatchEventKinds.ENTRY_MODIFY
                                && event.context().toString().equals(filePath.getFileName().toString())) {
                            logger.info("File " + filePath + " has changed.");
                            System.exit(1);
                            // Handle the file change (e.g., reload the context)
                        }
                    }
                    key.reset();
                }

                logger.info("Sleeping...");
                //Thread.sleep(5000);
            }
            // Keep the application running for a while to process routes
            //Thread.sleep(5000);

            // Stop the Camel context
            //camelContext.stop();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Close the Spring context
            applicationContext.close();
        }
    }

    public static List<Map<String, Object>> computeRouteStatus(MBeanServer mBeanServer, org.apache.camel.component.activemq.ActiveMQComponent amq) {
        List<Map<String, Object>> routesList = new ArrayList<>();

        try {
            Set<ObjectName> routes = mBeanServer.queryNames(new ObjectName("org.apache.camel:context=*,type=routes,*"), null);

            for (ObjectName route : routes) {
                Map<String, Object> routeAttributes = new HashMap<>();
                routeAttributes.put("name", route.getKeyProperty("name"));

                // Get MBean info to retrieve all attributes
                MBeanInfo mBeanInfo = mBeanServer.getMBeanInfo(route);
                MBeanAttributeInfo[] attributes = mBeanInfo.getAttributes();

                for (MBeanAttributeInfo attributeInfo : attributes) {
                    try {
                        if (attributeInfo.getName().equals("RouteProperties")) {
                            continue;
                        }
                        Object attributeValue = mBeanServer.getAttribute(route, attributeInfo.getName());
                        if (attributeValue != null && !attributeValue.toString().equals("")
                                && !attributeValue.toString().equals("false")) {
                            routeAttributes.put(attributeInfo.getName(), attributeValue);
                        }

                    } catch (Exception e) {
                        routeAttributes.put(attributeInfo.getName(), "Failed to get attribute: " + e.getMessage());
                    }
                }

                

                routesList.add(routeAttributes);
            }
            ObjectMapper objectMapper = new ObjectMapper();
            String jsonString = objectMapper.writeValueAsString(routesList);
            //        logger.info("Routes JSON: " + jsonString);
            ProducerTemplate producerTemplate = amq.getCamelContext().createProducerTemplate();
            //producerTemplate.sendBody("jms:topic:ROUTE_STATUS", jsonString);
            Map<String,Object> headers = new HashMap<>();
            
            if (System.getenv().containsKey("workflow")) {
                headers.put("workflow", System.getenv().get("workflow"));
            }
            else{
                headers.put("workflow", "workflow not found.");
            }
            if (System.getenv().containsKey("serverId")) {
                headers.put("serverId", System.getenv().get("serverId"));
            }
            else{
                headers.put("serverId", "serverId not found.");
            }
            producerTemplate.sendBodyAndHeaders("jms:topic:ROUTE_STATUS", jsonString, headers);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return routesList;
    }
    public static String cleanSubscriptionString(String aString, Set<String> anAllTopics) throws Exception {
        String res = "";
        Set<String> topics = new HashSet<String>();
        for (String str : aString.split(",")) {
            if (str.length() > 0) {
                if (topics.contains((str))) {
                    logger.info("Duplicate topic:" + str + " in string:" + aString + ", ignoring it");
                } else if (anAllTopics != null && anAllTopics.contains(str)) {
                    throw new IllegalStateException("Topic " + str + " is duplicated between standart / singletons or persistent topics");
                }   else {
                    topics.add(str);
                    if (anAllTopics != null) {
                        anAllTopics.add(str);
                    }
                    res = res + str + ",";
                }
            }
        }
        if (res.length() > 0) {
            res = res.substring(0, res.length() - 1);
        }
        return res;
    }
    public static TopicsConfiguration fetchTopicsFromConfigFile() {
        
        try {
            Properties prop = new Properties();
            String propFileName = "APSB.properties";

            File curfile = new File("../Configs" + File.separatorChar + propFileName);
                        
            logger.info("Checking APSB.properties in the following folder:" + curfile.getAbsolutePath());
            if(!curfile.exists())
            {
                logger.info("File not found. Trying container location.");
                curfile = new File("./conf" + File.separatorChar + propFileName);
                logger.info("Checking APSB.properties in the following folder:" + curfile.getAbsolutePath());
            }
            
            InputStream inputStream = new FileInputStream(curfile);
            prop.load(inputStream);

            String standardtopics = "";
            String singletontopics = "";
            String singletontopicsnohistory = "";
            String persistenttopics = "";
            for (Entry<Object, Object> ent : prop.entrySet()) {
                if (ent.getKey().toString().startsWith("pubsub.standardtopics")) {
                    standardtopics += "," + ent.getValue().toString();
                }
                if (ent.getKey().toString().startsWith("pubsub.singletontopics")) {
                    singletontopics += "," + ent.getValue().toString();
                }
                if (ent.getKey().toString().startsWith("pubsub.persistenttopics")) {
                    persistenttopics += "," + ent.getValue().toString();
                }
                if (ent.getKey().toString().startsWith("pubsub.singletonwithouthistorytopics")) {
                    singletontopicsnohistory += "," + ent.getValue().toString();
                    singletontopics += "," + ent.getValue().toString();
                }
            }
            
            TopicsConfiguration item = new TopicsConfiguration();
            item.standardTopics = standardtopics;
            item.singletonTopics = singletontopics;
            item.singletonTopicsNoHistory = singletontopicsnohistory;
            item.persistentTopics = persistenttopics;
            
            return item;
        
        } catch (Exception e) {
            logger.info("Cannot load APSB.properties file, unknow error" + e.getMessage());
            return null;
        }
        
    }

    public static class TopicsConfiguration {
        
        public String standardTopics;
        
        public String singletonTopics;
        
        public String singletonTopicsNoHistory;
        
        public String persistentTopics;
        
    }
}
