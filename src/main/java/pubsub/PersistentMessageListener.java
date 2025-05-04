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
public class PersistentMessageListener implements MessageListener
{
    PersistenceUnit owner;
    String topic;
    
    public PersistentMessageListener(PersistenceUnit owner,String topic)
    {
        this.owner=owner;
        this.topic=topic;
    }
    
    @Override
    public void onMessage(Message msg)
    {
        owner.onMessage(msg,topic);
    }
    
}
