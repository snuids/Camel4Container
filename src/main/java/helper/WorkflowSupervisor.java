/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package helper;

import static helper.ProcessInfo.runtime;
import java.util.Calendar;
import java.util.logging.Level;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.camel.CamelContext;
import org.apache.camel.Route;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pubsub.jmx.SupervisorClient;

/**
 *
 * @author snuids
 */
public class WorkflowSupervisor extends Thread
{    
    static final Logger logger = LoggerFactory.getLogger("WorkflowSupervisor");

    org.apache.camel.component.jms.JmsComponent jmsBean;
    Connection                                  jmsConn=null;    
    long                                        launchTime=Calendar.getInstance().getTimeInMillis();   
    CamelContext                                camelcontext=null;
    SupervisorClient                            myClient=null;
    String                                      workflow;
    
    
    public  WorkflowSupervisor(org.apache.camel.component.jms.JmsComponent jmsBean,String jmxAddress)
    {
        this.jmsBean=jmsBean;
        /*camelcontext = (CamelContext) camelworker.CamelWorker.context.getBean("camel");
        if (camelcontext == null) {
            logger.error("Camel context not found. Check that the Camel Context is named 'camel' in the xml configuration file.");
            return;
        }**/
        
        myClient=new SupervisorClient("myclient",jmxAddress, 10000);
        workflow=System.getenv("WORKFLOW_NAME");
        if(workflow==null)
        {
            workflow="NONAME";
            logger.info("WORKFLOW_NAME environment variable not found.");
        }            
        
        this.start();
    } 
   
    @Override
    public void run()
    {
        logger.info("Starting WorkflowSupervisor");
        while(true)
        {            
            try
            {                
                Thread.sleep(5000);
                if(jmsConn==null)
                    CreateJMSConfiguration();
                    //jmsBean.getConfiguration().getConnectionFactory();
                if(jmsConn!=null)
                {
                    Session sess=jmsConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    
                    TextMessage tmes= sess.createTextMessage();
                    
                    JSONObject mainobj=new JSONObject();
                    mainobj.put("@timestamp", Calendar.getInstance().getTimeInMillis());
                    mainobj.put("eventType", "WorkflowStatus");
                    mainobj.put("@launchTime", launchTime);
                    
                
                    mainobj.put("memory", (runtime.totalMemory() - runtime.freeMemory()) / 1000000);
                    mainobj.put("freeMemory", ( runtime.freeMemory()) / 1000000);
                    
                    mainobj.put("os", System.getProperty("os.name"));
                    mainobj.put("arch", System.getProperty("os.arch"));
                    
                    mainobj.put("routes", myClient.toJSON().get("Routes"));
                    mainobj.put("workflow", workflow);


                    tmes.setText(mainobj.toJSONString());
                    sess.createProducer(sess.createTopic("WorkflowStatus")).send(tmes);
                    sess.close();
                    
                    //tmes.setText(string);
                }
            }
            catch (Exception ex)
            {
                logger.info("Unable to send workflow messages. Ex="+ex.getMessage());
            }
        }
    }

    private void CreateJMSConfiguration()
    {
        try
        {
            jmsConn=(Connection) jmsBean.getConfiguration().getConnectionFactory().createConnection();
            jmsConn.start();
        }
        catch (Exception ex)
        {
            logger.info("Unable to create JMS connection. Ex="+ex.getMessage(),ex);
            jmsConn=null;
        }
    }
    
}
