/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pubsub;

/**
 *
 * @author Arnaud Marchand
 */
class OneMessage implements Comparable<OneMessage>
{
    public long pubSubID;
    public String topic;
    public String message;
    
    public OneMessage(String topic,String message,long pubSubID)
    {
        this.topic=topic;
        this.message=message;
        this.pubSubID=pubSubID;
    }


    @Override
    public int compareTo(OneMessage t)
    {                
        return Long.valueOf(pubSubID).compareTo(Long.valueOf(t.pubSubID));
        
    }
}
