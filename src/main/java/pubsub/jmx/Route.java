/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pubsub.jmx;

import java.util.Date;
import javax.management.ObjectName;

/**
 *
 * @author Arnaud Marchand
 */
public class Route
{
     public ObjectName objectInfoName;
     public String routeId;
     public String description;
     public String state;
     public long exchangesCompleted;
     public long exchangesFailed;     
     public long totalExchanges;
     
     public Date statisticTimeStamp;
     
     
     public long minProcessingTime;
     public long maxProcessingTime;
     public long totalProcessingTime;
     public long lastProcessingTime;
     public long meanProcessingTime;
     public Date firstExchangeCompletedTimestamp;
     public Date firstExchangeFailureTimestamp;
     public Date lastExchangeCompletedTimestamp;
     public Date lastExchangeFailureTimestamp;
     
     public boolean suspendable=true;
}
