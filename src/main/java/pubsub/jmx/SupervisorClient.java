/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pubsub.jmx;

import java.io.IOException;
import java.net.MalformedURLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Arnaud Marchand
 */
public class SupervisorClient extends Thread
{
    static final Logger logger = LoggerFactory.getLogger("SupervisorClient");
    
    String  id;
    String  address;
    Boolean interrupted=false;
    Boolean connected=false;

    int pollingSpeed=10000;
    JMXServiceURL url;
    JMXConnector jmxc;
    MBeanServerConnection conn;
    LinkedHashMap<String,Route> routes=new LinkedHashMap<String,Route>();
    
    public SupervisorClient(String id, String address,int pollingSpeed)
    {
        logger.info("Starting JMX client:"+id+" Address:"+address+" pollingSpeed:"+pollingSpeed);
        
        this.pollingSpeed=pollingSpeed;
        this.id = id;
        this.address = address;
        try
        {
            url = new JMXServiceURL(address);
        }
        catch (MalformedURLException ex)
        {
            logger.error("Unable to start, error is " + ex.getMessage());
        }
        
        start();
    }
    @Override
    public void run()
    {
        logger.info("Wait for 10 seconds in order to let the Camel Start.");
        try
        {
            sleep(10000);
        }
        catch (InterruptedException ex)
        {
            java.util.logging.Logger.getLogger(SupervisorClient.class.getName()).log(Level.SEVERE, null, ex);
        }
        logger.info("Ready.");
        logger.info("Client running");
        while(!interrupted())
        {
            try
            {
                sleep(5000);
                if(!connected)
                {
                    try                    
                    {
                        synchronized(routes)
                        {
                            routes.clear();
                        }
                        if(jmxc!=null)
                        {
                            try
                            {
                                jmxc.close();
                            }
                            catch(Exception e)
                            {
                                logger.error("Unable to close JMX connection. Ex="+e.getMessage());
                            }
                        }
                        logger.info("Trying to connect to:"+url);
                        jmxc=JMXConnectorFactory.connect(url);
                        conn= jmxc.getMBeanServerConnection();
                        logger.info("Success...");
                        connected=true;
                    }
                    catch (IOException ex)
                    {
                        java.util.logging.Logger.getLogger(SupervisorClient.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
                    }
                    
                }
                if(connected)
                {
                    try
                    {
                        ObjectName objName = new ObjectName("org.apache.camel:type=routes,*");
                        List<ObjectName> cacheList = new LinkedList(conn.queryNames(objName, null));    
                        synchronized(routes)
                        {
                            for (Iterator<ObjectName> iter = cacheList.iterator(); iter.hasNext();)
                            {
                                objName = iter.next();
                                String keyProps = objName.getCanonicalKeyPropertyListString();
                                ObjectName objectInfoName = new ObjectName("org.apache.camel:" + keyProps);
                                

                                String routeId="",description="",state="";
                                try
                                {
                                     routeId= (String) conn.getAttribute(objectInfoName, "RouteId");
                                }                
                                catch (Exception ex)
                                {
                                    java.util.logging.Logger.getLogger(SupervisorClient.class.getName()).log(Level.SEVERE, null, ex);
                                }
                                if(routes.get(routeId)==null)
                                {
                                    routes.put(routeId,new Route());
                                }
                                Route curroute=routes.get(routeId);
                                curroute.objectInfoName=objectInfoName;
                                try
                                {
                                    curroute.description = (String) conn.getAttribute(objectInfoName, "Description");
                                }
                                catch (Exception ex)
                                {
                                    java.util.logging.Logger.getLogger(SupervisorClient.class.getName()).log(Level.SEVERE, null, ex);
                                }

                                try
                                {
                                    curroute.state = (String) conn.getAttribute(objectInfoName, "State");
                                }
                                catch (Exception ex)
                                {
                                    java.util.logging.Logger.getLogger(SupervisorClient.class.getName()).log(Level.SEVERE, null, ex);
                                }
                                
                                try
                                {
                                    curroute.firstExchangeCompletedTimestamp = (Date) conn.getAttribute(objectInfoName, "FirstExchangeCompletedTimestamp");
                                }
                                catch (Exception ex)
                                {
                                    java.util.logging.Logger.getLogger(SupervisorClient.class.getName()).log(Level.SEVERE, null, ex);
                                }
                                
                                try
                                {
                                    curroute.firstExchangeFailureTimestamp = (Date) conn.getAttribute(objectInfoName, "FirstExchangeFailureTimestamp");
                                }
                                catch (Exception ex)
                                {
                                    java.util.logging.Logger.getLogger(SupervisorClient.class.getName()).log(Level.SEVERE, null, ex);
                                }
                                
                                try
                                {
                                    curroute.lastExchangeCompletedTimestamp = (Date) conn.getAttribute(objectInfoName, "LastExchangeCompletedTimestamp");
                                }
                                catch (Exception ex)
                                {
                                    java.util.logging.Logger.getLogger(SupervisorClient.class.getName()).log(Level.SEVERE, null, ex);
                                }
                                
                                try
                                {
                                    curroute.lastExchangeFailureTimestamp = (Date) conn.getAttribute(objectInfoName, "LastExchangeFailureTimestamp");
                                }
                                catch (Exception ex)
                                {
                                    java.util.logging.Logger.getLogger(SupervisorClient.class.getName()).log(Level.SEVERE, null, ex);
                                }
                                
                                try
                                {
                                    curroute.exchangesCompleted = (Long) conn.getAttribute(objectInfoName, "ExchangesCompleted");
                                }
                                catch (Exception ex)
                                {
                                    java.util.logging.Logger.getLogger(SupervisorClient.class.getName()).log(Level.SEVERE, null, ex);
                                }
                                
                                try
                                {
                                    curroute.exchangesFailed = (Long) conn.getAttribute(objectInfoName, "ExchangesFailed");
                                }
                                catch (Exception ex)
                                {
                                    java.util.logging.Logger.getLogger(SupervisorClient.class.getName()).log(Level.SEVERE, null, ex);
                                }
                                
                                try
                                {
                                    curroute.totalExchanges = (Long) conn.getAttribute(objectInfoName, "ExchangesTotal");
                                }
                                catch (Exception ex)
                                {
                                    java.util.logging.Logger.getLogger(SupervisorClient.class.getName()).log(Level.SEVERE, null, ex);
                                }
                                
                                try
                                {
                                    curroute.lastProcessingTime = (Long) conn.getAttribute(objectInfoName, "LastProcessingTime");
                                }
                                catch (Exception ex)
                                {
                                    java.util.logging.Logger.getLogger(SupervisorClient.class.getName()).log(Level.SEVERE, null, ex);
                                }
                                
                                try
                                {
                                    curroute.maxProcessingTime = (Long) conn.getAttribute(objectInfoName, "MaxProcessingTime");
                                }
                                catch (Exception ex)
                                {
                                    java.util.logging.Logger.getLogger(SupervisorClient.class.getName()).log(Level.SEVERE, null, ex);
                                }
                                
                                try
                                {
                                    curroute.minProcessingTime = (Long) conn.getAttribute(objectInfoName, "MinProcessingTime");
                                }
                                catch (Exception ex)
                                {
                                    java.util.logging.Logger.getLogger(SupervisorClient.class.getName()).log(Level.SEVERE, null, ex);
                                }
                                
                                try
                                {
                                    curroute.meanProcessingTime = (Long) conn.getAttribute(objectInfoName, "MeanProcessingTime");
                                }
                                catch (Exception ex)
                                {
                                    java.util.logging.Logger.getLogger(SupervisorClient.class.getName()).log(Level.SEVERE, null, ex);
                                }
                                
                                try
                                {
                                    curroute.totalProcessingTime = (Long) conn.getAttribute(objectInfoName, "TotalProcessingTime");
                                }
                                catch (Exception ex)
                                {
                                    java.util.logging.Logger.getLogger(SupervisorClient.class.getName()).log(Level.SEVERE, null, ex);
                                }
                                curroute.statisticTimeStamp=new Date();
                                //System.out.println("Route:"+routeId+" Exchanges:"+curroute.exchangesCompleted+" State:"+curroute.state+" First:"+curroute.firstExchangeCompletedTimestamp+" Description:"+curroute.description);
                            }
                        }
                        if(cacheList.size()==0)
                        {
                            logger.info("No routes found. Resetting connection.");
                            connected=false;
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.error("Error while retrieving routes. Resetting connection");
                        connected=false;
                    }
                    //System.out.println(">>>>>>>>>>>>>");
                    //System.out.println(toJSON());
                    //System.out.println(">>>>>>>>>>>>>");
                }
            }
            catch (InterruptedException ex)
            {
                java.util.logging.Logger.getLogger(SupervisorClient.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
            }
            
        }
        logger.info("Client stopped");
    }
    public void stopClient()
    {
        logger.info("Stopping client");
        interrupted=true;
    }
    public JSONObject toJSON()
    {
        
        SimpleDateFormat simpleformat=new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        JSONObject mainobj=new JSONObject();
        
        JSONArray list = new JSONArray();
        mainobj.put("Routes", list);
        synchronized(routes)
        {
            ArrayList<String> listkeys=new ArrayList<String>();
        
            for(String key: routes.keySet())
                listkeys.add(key);
        
            java.util.Collections.sort(listkeys);
            
            for(String routeid:listkeys)
            {
                Route rou=routes.get(routeid);
                JSONObject routeobj=new JSONObject();
                routeobj.put("id", routeid);
                
                //routeobj.put("description", rou.description);
                routeobj.put("exchangesCompleted", rou.exchangesCompleted);
                routeobj.put("exchangesFailed", rou.exchangesFailed);
                
                
                if(rou.firstExchangeCompletedTimestamp!=null)
                {
                    routeobj.put("firstExchangeCompletedTimestamp", simpleformat.format(rou.firstExchangeCompletedTimestamp));
                    routeobj.put("firstExchangeCompletedTimestampTS", rou.firstExchangeCompletedTimestamp.getTime());
                }
                
                if(rou.firstExchangeFailureTimestamp!=null)
                {
                    routeobj.put("firstExchangeFailureTimestamp", simpleformat.format(rou.firstExchangeFailureTimestamp));
                    routeobj.put("firstExchangeFailureTimestampTS",rou.firstExchangeFailureTimestamp.getTime());
                }
                
                if(rou.lastExchangeCompletedTimestamp!=null)
                {
                    routeobj.put("lastExchangeCompletedTimestamp", simpleformat.format(rou.lastExchangeCompletedTimestamp));
                    routeobj.put("lastExchangeCompletedTimestampTS", rou.lastExchangeCompletedTimestamp.getTime());
                }
                
                if(rou.lastExchangeFailureTimestamp!=null)
                {
                    routeobj.put("lastExchangeFailureTimestamp", simpleformat.format(rou.lastExchangeFailureTimestamp));
                    routeobj.put("lastExchangeFailureTimestampTS", rou.lastExchangeFailureTimestamp.getTime());
                }
                
                
                
                routeobj.put("lastProcessingTime", rou.lastProcessingTime);
                routeobj.put("maxProcessingTime", rou.maxProcessingTime);
                routeobj.put("meanProcessingTime", rou.meanProcessingTime);
                routeobj.put("minProcessingTime", rou.minProcessingTime);
                routeobj.put("state", rou.state);
                
                routeobj.put("statisticTimeStamp", simpleformat.format(rou.statisticTimeStamp));
                routeobj.put("statisticTimeStampTS", rou.statisticTimeStamp.getTime());
                routeobj.put("totalExchanges", rou.totalExchanges);
                routeobj.put("totalProcessingTime", (rou.totalProcessingTime/1000));
                          
                list.add(routeobj);
            }
        }
        return mainobj;
    }

    String callJMXMethod(String command,String route)
    {
        if(!connected)
        {
            return "ERR:Not connected";
        }
        
        synchronized(routes)
        {
            if(!routes.containsKey(route))
            {
                return "ERR:Unknown route";
            }
            Route rou=routes.get(route);
            if(!rou.suspendable)
            {
                if("resume".compareTo(command)==0)
                    command="start";
                else if("suspend".compareTo(command)==0)
                    command="stop";
            }
            logger.info("Invoking reset stats...");
            try
            {
                conn.invoke(rou.objectInfoName, command,null, null);
            }
            catch (InstanceNotFoundException ex)
            {
                java.util.logging.Logger.getLogger(SupervisorClient.class.getName()).log(Level.SEVERE, null, ex);
            }
            catch (MBeanException ex)
            {
                
                if("suspend".compareTo(command)==0)
                {
                    logger.info("Setting route to not suspensable.");
                    rou.suspendable=false;
                    callJMXMethod(command,route);
                }
                else if("resume".compareTo(command)==0)
                {
                    logger.info("Setting route to not suspensable.");
                    rou.suspendable=false;
                    callJMXMethod(command,route);
                }
                else
                {
                    java.util.logging.Logger.getLogger(SupervisorClient.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            catch (ReflectionException ex)
            {
                java.util.logging.Logger.getLogger(SupervisorClient.class.getName()).log(Level.SEVERE, null, ex);
            }
            catch (IOException ex)
            {
                java.util.logging.Logger.getLogger(SupervisorClient.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return "OK";
    }
    
}
