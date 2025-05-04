/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pubsub.jmx;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Arnaud Marchand
 */
public class Supervisor
{
    static final Logger logger = LoggerFactory.getLogger("Supervisor");
    int pollingSpeed;
    Map<String,String> clientNames=new LinkedHashMap<String,String>();
    Map<String,SupervisorClient> clients=new LinkedHashMap<String,SupervisorClient>();
    
    public Supervisor()
    {
        logger.info("JMX:Creating JMX Supervisor.");
            
    }

    public int getPollingSpeed()
    {
        return pollingSpeed;
    }

    public void setPollingSpeed(int pollingSpeed)
    {
        this.pollingSpeed = pollingSpeed;
    }
    
    
    
    public Map<String,String> getClientNames()
    {
        return clientNames;
    }

    public void run()
    {
        logger.info("JMX:Starting JMX Supervisor.");
        for(String key:this.clientNames.keySet())
        {
            if(key.length()==0)
                continue;
            logger.info("JMX:Key="+key+" Value="+this.clientNames.get(key));
            if(clients.containsKey(key))
            {
                logger.error("JMX:"+key+" already defined.");
            }
            else
            {
                clients.put(key,new SupervisorClient(key, this.clientNames.get(key),pollingSpeed));
            }
        }  
    }
    
    public void setClientNames(Map<String,String> clientNames)
    {
        this.clientNames = clientNames;
    }
      
    public String toJson()
    {
        JSONObject mainobj=new JSONObject();
        
        JSONArray list = new JSONArray();
        mainobj.put("clients", list);
        
        ArrayList<String> listkeys=new ArrayList<String>();
        
        for(String key: this.clients.keySet())
            listkeys.add(key);
        
        java.util.Collections.sort(listkeys);
        
        for(String key: listkeys)
        {
            SupervisorClient client=this.clients.get(key);
            JSONObject cliobj=new JSONObject();
            cliobj.put("id", key);
            cliobj.put("data",client.toJSON());
            
            list.add(cliobj);
        }
        
        return mainobj.toJSONString();          
    }
    
    public String callJMXMethod(String command,String client,String route)
    {
        logger.info("Call JMX:"+command+" of:"+client+" route:"+route);
        synchronized(clients)
        {
            if(!clients.containsKey(client))
                return "ERR:Unknown Client.";
            else
                return clients.get(client).callJMXMethod(command,route);
        }
    }
}
