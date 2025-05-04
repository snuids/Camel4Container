package pubsub;

import java.util.Map;
import jakarta.jms.JMSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Arnaud Marchand
 */
public class ResynchClientThread extends Thread
{
    static final Logger logger = LoggerFactory.getLogger("ResynchClientThread");
    
    Map<String,ResynchTopic> resynchTopicList;
    long endID;
    String inEndTime;
    String resynchQueue;
    Dispatcher dispatcher;

    ResynchClientThread(Map<String,ResynchTopic> aResynchTopicList
            ,long anEndID, String anInEndTime
            ,String aResynchQueue,Dispatcher aDispatcher)
    {
        super();
        logger.info("Initializing resynch thread");
        resynchTopicList=aResynchTopicList;
        endID=anEndID;
        inEndTime=anInEndTime;
        resynchQueue=aResynchQueue;
        dispatcher=aDispatcher;
    }
    @Override
    public void run()
    {
        logger.info("Starting resynch thread");
        try
        {
            dispatcher.resynchClient(resynchTopicList, endID, inEndTime, resynchQueue);
        }
        catch (JMSException ex)
        {
            logger.error("Error while running Resynch Thread. Ex="+ex.getMessage(),ex);
        }
        logger.info("Finishing resynch thread");
    }

}