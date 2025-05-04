/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pubsub;

import jakarta.jms.Message;
import jakarta.jms.MessageListener;

/**
 *
 * @author Arnaud Marchand
 */
public class ReplicationMessageListener implements MessageListener
{
    Dispatcher owner;
    String topic;
    
    public ReplicationMessageListener(Dispatcher owner,String topic)
    {
        this.owner=owner;
        this.topic=topic;
    }
    
    @Override
    public void onMessage(Message msg)
    {
        owner.onMessageForReplication(msg,topic);
    }
    
}
