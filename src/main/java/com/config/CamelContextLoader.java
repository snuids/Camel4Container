package com.config;

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
import java.util.concurrent.TimeUnit;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.fasterxml.jackson.databind.ObjectMapper;


public class CamelContextLoader {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java -jar <jarfile> path/to/camel-context.xml");
            System.exit(1);
        }

        Map<String, String> env = System.getenv();
        for (Map.Entry<String, String> entry : env.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }

        String path = "file:" + args[0];
        Path filePath = Paths.get(args[0]);

        // Load the Spring context from the external XML file
        ClassPathXmlApplicationContext applicationContext
                = new ClassPathXmlApplicationContext(path);

        // Get the Camel context from the Spring context
        CamelContext camelContext = applicationContext.getBean(CamelContext.class);
        org.apache.camel.component.activemq.ActiveMQComponent amq = applicationContext.getBean(org.apache.camel.component.activemq.ActiveMQComponent.class);

        try {
            // Start the Camel context
            camelContext.start();
            // Monitor the file for changes
            WatchService watchService = FileSystems.getDefault().newWatchService();
            filePath.getParent().register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
            System.out.println("Camel context started...");
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
                            System.out.println("File " + filePath + " has changed.");
                            System.exit(1);
                            // Handle the file change (e.g., reload the context)
                        }
                    }
                    key.reset();
                }

                System.out.println("Sleeping...");
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
                routeAttributes.put("name", route.toString());

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
            //        System.out.println("Routes JSON: " + jsonString);
            ProducerTemplate producerTemplate = amq.getCamelContext().createProducerTemplate();
            //producerTemplate.sendBody("jms:topic:ROUTE_STATUS", jsonString);
            Map<String,Object> headers = new HashMap<>();
            
            if (System.getenv().containsKey("camelName")) {
                headers.put("camelName", System.getenv().get("camelName"));
            }
            if (System.getenv().containsKey("serverId")) {
                headers.put("serverId", System.getenv().get("serverId"));
            }
            producerTemplate.sendBodyAndHeaders("jms:topic:ROUTE_STATUS", jsonString, headers);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return routesList;
    }
}
