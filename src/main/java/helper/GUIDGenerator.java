/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package helper;

import java.util.Date;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates a unique ID per server. This unique ID can be used to simulate
 * the old message ID of the previous Pub/Sub system
 * @author Arnaud Marchand
 */
public class GUIDGenerator
{
    static final Logger logger = LoggerFactory.getLogger("GUIDGenerator");

    long serverID=0;    
    long curposition=621355968000000000L+System.currentTimeMillis()*10000+serverID;
    long curlocposition=9999;
    
    public long getServerID()
    {
        return serverID;
    }

    public void setServerID(long serverID)
    {
        this.serverID = serverID;
    }
               
    public GUIDGenerator()
    {
        logger.info("GUID Generator initialized.");
    }
    
    /**
     * Computes a unique ID matching the .Net UTC time logic and ending with the server ID<br/>
     * @return the UTC unique ID
     */
    public long nextUniqueID()
    {
        synchronized(this)
        {
            Date date=new Date();
            date.getTime();
            //GregorianCalendar cal = new GregorianCalendar(); 
            //cal.setTimeInMillis(utcMiliseconds);
            //Date localTime = new Date(utcMiliseconds + cal.get(Calendar.ZONE_OFFSET) + cal.get(Calendar.DST_OFFSET));
            long newposition=621355968000000000L+System.currentTimeMillis()*10000+serverID;
            if(newposition<=curposition)
            {
                curposition= curposition+10;
            }
            else
                curposition=newposition;
                
            return curposition;
        }
    }
    /**
     * Computes a unique ID matching the .Net local time logic and ending with the server ID<br/>
     * @return the UTC unique ID
     */
    public long nextLocalUniqueID()
    {
        synchronized(this)
        {
            Date date=new Date();
            date.getTime();
            //GregorianCalendar cal = new GregorianCalendar(); 
            //cal.setTimeInMillis(utcMiliseconds);
            //Date localTime = new Date(utcMiliseconds + cal.get(Calendar.ZONE_OFFSET) + cal.get(Calendar.DST_OFFSET));
            long curtime=System.currentTimeMillis();
            
            
            DateTime dt=new DateTime(curtime);
            long offsetHours = (dt.getZone().getOffset(dt)/1000)/(3600);

            
            long newposition=621355968000000000L+(System.currentTimeMillis()+(offsetHours*3600000))*10000+serverID;
            if(newposition<=curlocposition)
            {
                curlocposition= curlocposition+10;
            }
            else
                curlocposition=newposition;
                
            return curlocposition;
        }
    }
}
