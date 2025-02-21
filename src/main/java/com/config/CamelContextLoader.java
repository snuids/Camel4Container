package com.config;

import org.apache.camel.CamelContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class CamelContextLoader {
    public static void main(String[] args) {
        for (String arg : args) {
            System.out.println("Argument: " + arg);
        }
        // Load the Spring context from the external XML file
        ClassPathXmlApplicationContext applicationContext = 
                //new ClassPathXmlApplicationContext("META-INF/spring/camel-context.xml");
                new ClassPathXmlApplicationContext("file:/Users/snuids/Desktop/Camel4/camelrunner2/camel/data/camel-context.xml");

        // Get the Camel context from the Spring context
        CamelContext camelContext = applicationContext.getBean(CamelContext.class);

        try {
            // Start the Camel context
            camelContext.start();
            while(true){
                System.out.println("Camel context started...");
                Thread.sleep(5000);
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
}