package com.equans.camel.component.pubsub;

import org.apache.camel.Endpoint;
import org.apache.camel.Processor;
import org.apache.camel.SuspendableService;
import org.apache.camel.support.DefaultConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubConsumer extends DefaultConsumer implements SuspendableService {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubConsumer.class);
    
    private Boolean onlyWhenIsLocalMaster = false;

    public PubSubConsumer(Endpoint endpoint, Processor processor) {
        super(endpoint, processor);
    }
    

    @Override
    protected void doStart() throws Exception {
        
    }
    
    
    @Override
    protected void doStop() throws Exception {
        
    }

    public Boolean needNotification(Boolean isLocalMaster) {
        if (onlyWhenIsLocalMaster == false) {
            return true;
        }
        
        return isLocalMaster == onlyWhenIsLocalMaster;
    }

    /**
     * @param OnlyWhenIsLocalMaster the OnlyWhenIsLocalMaster to set
     */
    public void setOnlyWhenIsLocalMaster(Boolean OnlyWhenIsLocalMaster) {
        this.onlyWhenIsLocalMaster = OnlyWhenIsLocalMaster;
    }
    
}
