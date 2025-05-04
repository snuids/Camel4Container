/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pubsub;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Arnaud Marchand
 */
public class ResynchTopic
{
    static final org.slf4j.Logger logger = LoggerFactory.getLogger("ResynchTopic");

    Date startDate;
    long pubSubID;
    String topic;
        
    public ResynchTopic(String []aResynchTopic)
    {
        try
        {
            SimpleDateFormat formatter = new SimpleDateFormat("ddMMyyyyHHmmssS");
            topic=aResynchTopic[0];
            pubSubID=Long.parseLong(aResynchTopic[1]);
            startDate=formatter.parse(aResynchTopic[2]);
        }
        catch (Exception ex)
        {
            logger.error("Unable to create resynch topic. Ex="+ex.getMessage(),ex);
        }
    }
}
