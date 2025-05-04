package com.equans.pubsub.client;

import java.util.Date;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates a unique ID per server. This unique ID can be used to simulate the
 * old message ID of the previous Pub/Sub system
 *
 * @author Arnaud Marchand
 */
public class GuidGenerator {

    static final Logger logger = LoggerFactory.getLogger("GUIDGenerator");

    private long serverId = 0;
    private long curposition = 621355968000000000L + System.currentTimeMillis() * 10000 + serverId;
    private long curlocposition = 9999;
    private long hashcode = 0;

    public long getServerId() {
        return serverId;
    }

    public void setServerId(long serverID) {
        this.serverId = serverID;
    }

    public GuidGenerator(String subsriberId) {
        logger.info("GUID Generator initialized.");
        this.hashcode = (subsriberId.hashCode()%100)*100;
        if (this.hashcode<0) {
            this.hashcode = -1*this.hashcode;
        }
    }

    /**
     * Computes a unique ID matching the .Net UTC time logic and ending with the
     * server ID<br/>
     *
     * @return the UTC unique ID
     */
    public long nextUniqueID() {
        synchronized (this) {
            Date date = new Date();
            date.getTime();
            long newposition = 621355968000000000L + System.currentTimeMillis() * 10000 + serverId;
            if (newposition <= curposition) {
                curposition = curposition + 10;
            } else {
                curposition = newposition;
            }

            return curposition;
        }
    }

    /**
     * Computes a unique ID matching the .Net local time logic and ending with
     * the server ID<br/>
     *
     * @return the UTC unique ID
     */
    public long nextLocalUniqueID() {
        synchronized (this) {
            long curtime = System.currentTimeMillis();
            DateTime dt = new DateTime(curtime);
            
            long offsetHours = (dt.getZone().getOffset(dt) / 1000) / (3600);
            long newposition = 621355968000000000L + hashcode + (System.currentTimeMillis() + (offsetHours * 3600000)) * 10000 + serverId;
            if (newposition <= curlocposition) {
                curlocposition = curlocposition + 10;
            } else {
                curlocposition = newposition;
            }

            return curlocposition;
        }
    }
}
