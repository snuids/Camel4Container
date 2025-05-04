package com.equans.pubsub.client.interfaces;

import com.equans.pubsub.client.PubSubClient.JMS_STATUS;
import com.equans.pubsub.client.PubSubClient.STATE;
import java.util.Map;

/**
 *  Interface that define the behavior of JavaPubSub
 * @author Carnus
 */
public interface IPubSubClientReceiver {
    
    void onMessageReceived(String topic, String message, Map<String, Object> properties);
    
    void onBinaryMessageReceived(String topic, byte[] data, Map<String, Object> properties);
    
    void onStateChanged(STATE state);
    
    void onJMSStatusChanged(JMS_STATUS status);
    
    Map<String,String> onFillMessage();
    
}
