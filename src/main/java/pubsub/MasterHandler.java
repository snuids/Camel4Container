/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pubsub;

import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Arnaud Marchand
 */
public class MasterHandler implements Processor
{
    static final Logger logger = LoggerFactory.getLogger("MasterHandler");

    boolean Master=false;
    public Date  lastMasterReceived=new Date();
    public Date  masterSinceDate=new Date();
    public int serverID;    
    public long timeShift=0;
    
    public boolean isMaster()
    {
        return Master;
    }

    public void setMaster(boolean Master)
    {
        if(this.Master!=Master)
        {
            logger.info("State is now:"+(Master?"Master":"Slave"));
        }
        
        if((!this.Master)&&(Master))
        {
            masterSinceDate=new Date();
        }        
        this.Master = Master;
    }

    @Override
    public void process(Exchange exchng) throws Exception
    {        
        
        String message=(String)exchng.getIn().getBody();
        int indexofpipe = message.indexOf('|');
        if(indexofpipe==-1)
        {
            logger.error("Unable to decode Master.");

            return;
        }
        try
        {
            if(message.startsWith("CLOCK"))  // no need to decode
                return;
            int senderid;
            int indexofsharp = message.indexOf('#');
            if (indexofsharp == -1)
            {
                senderid = Integer.parseInt(message.substring(indexofpipe + 1,indexofpipe+2));
            }
            else
            {
                senderid = Integer.parseInt(message.substring(indexofpipe + 1, indexofsharp ));
            }
            if (senderid == serverID)
            {
                if (message.startsWith("FORCEMASTER"))
                {
                    if (!isMaster())
                    {                        
                        logger.info("Forced To Master.");
                        setMaster(true);
                    }
                }
//                    theLogger.Warn("Sent by me ignore it...");
            }
            else
            {
                int     receivedrequest=-1;
                long    receivedtimemaster=-1;
                
                Long otherservertimestamp=(Long)exchng.getIn().getHeaders().get("GenerationTimeStamp");
                if(otherservertimestamp!=null)
                {
                    timeShift=new Date().getTime()-otherservertimestamp.longValue();
                }
                        
                if (message.startsWith("MASTER"))
                {
                    if(indexofsharp==-1)
                    {
                        logger.error("Unable to decode Master Start Date.");

                        return;
                    }
                    try
                    {
                        receivedtimemaster= Long.parseLong(message.substring(indexofsharp+1));
                        if(receivedtimemaster<masterSinceDate.getTime())
                        {
                            logger.info("Setting other master date.(1)");
                            lastMasterReceived=new Date();
                        }
                        
                        if(isMaster()&&(receivedtimemaster<masterSinceDate.getTime()))
                        {
                            logger.info("Receive a message from an older master. Becoming slave.");
                            setMaster(false);
                            lastMasterReceived=new Date();
                            
                        }
                        
                        if(!isMaster())
                        {
                            logger.info("Setting other master date.(1)");
                            lastMasterReceived=new Date();
                        }
                    }
                    catch(Exception e)
                    {
                        logger.error("Unable to decode Master Start Date.");
                        return;
                    }
                    
                }
                if (message.startsWith("FORCEMASTER"))
                {
                    logger.info("Master forced. Becoming slave.");
                    lastMasterReceived=new Date();
                    setMaster(false);
                }             
            }
        }
        catch(Exception e)
        {
            logger.error("Unable to decode Master Start Date. Ex=" + e.getMessage());

        }
    }
    
}
