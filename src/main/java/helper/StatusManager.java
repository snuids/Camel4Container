/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package helper;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author snuids
 */
public class StatusManager
{
    static final Logger logger = LoggerFactory.getLogger("StatusManager");
    
    final HashMap<String,Boolean> cacheFormulas=new HashMap<String,Boolean>();
    final HashMap<String,Boolean> cacheStatus=new HashMap<String,Boolean>();
    
    public String ExceptionRegex="";

    
    public void feedWithStatus(String aStatus)
    {
//        logger.info("==>"+ExceptionRegex);
        Boolean oneThingChanged=false;
        try
        {
            JsonObject jobject = new JsonParser().parse(aStatus).getAsJsonObject();
            String source=jobject.get("source").getAsString();
            if(source.startsWith("SUB"))
            {
                JsonArray array=jobject.getAsJsonArray("tags");
                for(JsonElement obj:array)
                {
                    String tagName=obj.getAsJsonObject().get("tagName").getAsString();
                    int tagValue=obj.getAsJsonObject().get("tagValue").getAsInt();
                    boolean hasChanged=obj.getAsJsonObject().get("hasChanged").getAsBoolean();
                    
//                    logger.info("TAG:"+tagName+" =>"+tagValue);
                    
                    boolean newvalue=false;
                    
                    synchronized(cacheStatus)
                    {
                        if(cacheStatus.containsKey(tagName))
                        {
                            
                            
                            if((ExceptionRegex.compareTo("")!=0)&&(tagName.matches(ExceptionRegex)))
                            {
                                newvalue=tagValue<2;
//                                logger.info("================>TAG:"+tagName+"=>"+tagValue+"=>"+newvalue);
                            }                                                            
                            else if(tagName.indexOf("CHU")>=0)                        
                                newvalue=tagValue<8;                        
                            else
                                newvalue=tagValue<4;
                            
                            if(cacheStatus.get(tagName)!=newvalue)
                                hasChanged=true;                                
                        }
                    }
                    
                    if((hasChanged)||(!cacheStatus.containsKey(tagName)))
                    {
                        oneThingChanged=true;
                        synchronized(cacheStatus)
                        {
/*                            if(tagName.indexOf("CHU")>=0)                        
                                cacheStatus.put(tagName, tagValue<8);                        
                            else
                                cacheStatus.put(tagName, tagValue<4);*/
                            cacheStatus.put(tagName, newvalue);
                        }
                    }
                }
            }
            
        }
        catch(Exception e)
        {
            logger.error("Unable to decode status. Ex="+e.getMessage(),e);
        }
        if(oneThingChanged)
        {
            synchronized(cacheFormulas)
            {
                cacheFormulas.clear();
            }
        }
    }
    public boolean resolveStatus(String aStatus)
    {
        aStatus=aStatus.split("\n")[0];
        if(aStatus.compareTo("")==0)
            return true;
        synchronized(cacheFormulas)
        {
            if(cacheFormulas.containsKey(aStatus))
            {
//                logger.info("Returning cached value for formula:"+aStatus);
                return cacheFormulas.get(aStatus);
            }
        }
        
        try
        {
            for(String stat:aStatus.split("&"))
            {
                synchronized(cacheStatus)
                {
                    if(cacheStatus.containsKey(stat))
                    {
                        if(!cacheStatus.get(stat))
                        {
                            updateCacheStatus(aStatus,false);
                            return false;
                        }                        
                    }
                    else
                    {
                        logger.error("Unknown key:"+stat+" Formula:"+aStatus);
                        {
                            updateCacheStatus(aStatus,false);
                            return false;
                        }
                    }
                }
            }
        }
        catch(Exception e)
        {
            logger.error("Unable to resolve status. Ex="+e.getMessage(),e);
            updateCacheStatus(aStatus,false);
            return false;
        }
        updateCacheStatus(aStatus,true);
        return true;
    }

    private void updateCacheStatus(String aStatus, boolean b)
    {
        synchronized(cacheFormulas)
        {
            cacheFormulas.put(aStatus,b);
        }
    }
}
