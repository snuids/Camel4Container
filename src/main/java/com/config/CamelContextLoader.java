package com.config;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

import org.apache.camel.CamelContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class CamelContextLoader {
    public static void main(String[] args) {
        if (args.length <1) {
            System.out.println("Usage: java -jar <jarfile> path/to/camel-context.xml");
            System.exit(1);
        }
        String path = "file:"+args[0];
        Path filePath = Paths.get(args[0]);
        
        // Load the Spring context from the external XML file
        ClassPathXmlApplicationContext applicationContext = 
                new ClassPathXmlApplicationContext(path);

        // Get the Camel context from the Spring context
        CamelContext camelContext = applicationContext.getBean(CamelContext.class);

        try {
            // Start the Camel context
            camelContext.start();
            // Monitor the file for changes
            WatchService watchService = FileSystems.getDefault().newWatchService();
            filePath.getParent().register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
            System.out.println("Camel context started...");

            while(true){
                WatchKey key = watchService.take();
                for (WatchEvent<?> event : key.pollEvents()) {
                    if (event.kind() == StandardWatchEventKinds.ENTRY_MODIFY &&
                            event.context().toString().equals(filePath.getFileName().toString())) {
                        System.out.println("File " + filePath + " has changed.");
                        System.exit(1);
                        // Handle the file change (e.g., reload the context)
                    }
                }
                key.reset();
                System.out.println("Sleeping...");
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