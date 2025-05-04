/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pubsub;

//import camelworker.CamelWorker;
import camelworker.CamelWorker;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.jms.JmsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.*;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Properties;
/**
 *
 * @author Arnaud Marchand
 * 
 * SingletonName=GLOBAL_PROP
ResynchQueue=NEWREQUEUE
Action=Resynch
 * 
 * 
 * 
 */
public class BSSingleton implements Processor,MessageListener
{
    static final Logger logger = LoggerFactory.getLogger("BSSingleton");
    String directory;    
    Connection connection=null;
    SimpleDateFormat previousversionformat=new SimpleDateFormat("ddMMMyyyy_HHmmss");
    LinkedHashMap<String, String> singletonswithouthistory=new LinkedHashMap<String, String>();
    
    @Override
    public void process(Exchange exchng) throws Exception
    {
        File dir = new File(directory);
    
        if(!dir.exists())
            dir.mkdir();
        
        String HeaderAction=null;
        String WriteFileName=null;
        
        if(exchng.getIn().hasHeaders())
        {
            HeaderAction=(String)exchng.getIn().getHeader("Action");
            if((HeaderAction!=null)&&(HeaderAction.compareTo("WriteFile")==0))
            {
                WriteFileName=(String)exchng.getIn().getHeader("SingletonFileName");
                logger.info("Received a write command for file:"+WriteFileName);
                                         
            
                FileWriter f = new FileWriter(directory+WriteFileName);
                PrintWriter out = new PrintWriter(f);
                out.print(exchng.getIn().getBody());
                out.close();
                return;
            }
        } 
        
        
        String res=exchng.getFromEndpoint().getEndpointUri();
        String[] split=res.split(":");
        String topic=split[split.length-1].split("\\?")[0];
        String resc=(String)exchng.getIn().getBody();
        
        if(topic.compareTo("SINGLETON_CONTROL")==0)
        {
            logger.info("Control message rerceived:"+resc);
            Properties properties = new Properties();
            properties.load(new StringReader(resc));
            
            String Action=properties.getProperty("Action");
            if(Action!=null)
            {
                if(connection==null)
                {
                    try
                    {
                        org.apache.camel.component.jms.JmsComponent comp
                        =(org.apache.camel.component.jms.JmsComponent)CamelWorker.context.getBean("jms");
                        JmsConfiguration conf= comp.getConfiguration();
                        ConnectionFactory fact=conf.getConnectionFactory();
                        connection= fact.createConnection();
                        connection.setClientID("BSSingleton");
                        connection.start();
                    }
                    catch(Exception e)
                    {
                        logger.info("Error. Ex="+e.getMessage(),e);
                        try
                        {
                            connection.stop();

                        }
                        catch(Exception e2)
                        {

                        }
                        connection=null;
                    }
                }
                if(connection==null)
                {
                    logger.error("Unable to create connection.");
                    return;
                }
                
                String ResynchQueue=properties.getProperty("ResynchQueue");

                
                if(Action.compareTo("MatchSingletonsStartingWith")==0)
                {
                    logger.info("MatchSingletonsStartingWith command received.");
                    String MatchString=properties.getProperty("MatchString");
                    File actual = new File(directory);
                    
                    Session sess=connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    Destination destination=sess.createQueue(ResynchQueue);
                    MessageProducer producer = sess.createProducer(destination);
                    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                    
                    for( File f : actual.listFiles())
                    {
                        if(f.getName().startsWith(MatchString))
                        {
                            TextMessage message;

                            logger.info("Forwarding:"+f.getName());
                            try
                            {
                                String resstr=readFileAsString(f.getAbsolutePath());
                                message = sess.createTextMessage("Queue="+f.getName().substring(0, f.getName().length()-4) +"\r\n"+resstr);
                                producer.send(message);

                                logger.info("Message sent.");
                            }
                            catch(Exception e)
                            {
                                logger.info("Singleton "+f.getName()+" not found. Ex="+e.getMessage());

                            }
                        }
                    }
                    
                    producer.close();
                    sess.close();
                }   
                else
                {
                    String SingletonName=properties.getProperty("SingletonName");
                    
                    Session sess=connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    Destination destination=sess.createQueue(ResynchQueue);
                    MessageProducer producer = sess.createProducer(destination);
                    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                    TextMessage message;

                    try
                    {
                        String resstr=readFileAsString(directory+SingletonName+".txt");
                        message = sess.createTextMessage("Queue="+SingletonName+"\r\n"+resstr);
                        producer.send(message);

                        logger.info("Message sent.");
                    }
                    catch(Exception e)
                    {
                        logger.info("Singleton "+SingletonName+" not found. Ex="+e.getMessage());

                    }
                    message = sess.createTextMessage("Action=EndOfResync\r\nTopic="+SingletonName);
                    producer.send(message);
                    producer.close();
                    sess.close();
                }
            }
            
            return;
        }

                                 
        logger.info("Received:"+res);
        FileWriter f = new FileWriter(directory+topic.split("\\?")[0]+".txt");
        PrintWriter out = new PrintWriter(f);
        out.print(resc);
        out.close();
    }

    public String getSingletonContent(String aFileName) throws IOException
    {
        return readFileAsString(directory+aFileName);
    }
    public  String readFileAsString(String filePath) throws java.io.IOException{
    byte[] buffer = new byte[(int) new File(filePath).length()];
    BufferedInputStream f = null;
    try {
        f = new BufferedInputStream(new FileInputStream(filePath));
        f.read(buffer);
    } finally {
        if (f != null) try { f.close(); } catch (IOException ignored) { }
    }
    return new String(buffer);
}
    
    public String getDirectory()
    {
        return directory;
    }

    public void setDirectory(String directory)
    {
        this.directory = directory;
    }

    @Override
    public void onMessage(Message msg)
    {
        logger.info("Singleton Message received.");
        try 
        {
        
            String MessageAsText="";

            if(msg instanceof BytesMessage)
            {
                    BytesMessage byteMessage=(BytesMessage)msg;
                    byte[] byteArr = new byte[(int)byteMessage.getBodyLength()];
                    byteMessage.readBytes(byteArr);
                    MessageAsText = new String(byteArr);//, "UTF-16");          
            }
            else if(msg instanceof TextMessage)
            {
                MessageAsText=((TextMessage)msg).getText();
            }
            else if (!(msg instanceof TextMessage)) {
                return;
            }


            try
            {
                Message mes= msg;
                Destination des=mes.getJMSDestination();
                String destination=des.toString().replace("topic://","");
                String user="";
                String prop=mes.getStringProperty("User");
                if((prop!=null)&&(prop.length()>0))
                {
                    user="("+prop+")";
                }
                // first back up file

                if(!singletonswithouthistory.containsKey(destination))
                {
                    /*File afile =new File(directory+destination+".txt");

                    if(afile.exists())
                    {

                        if(afile.renameTo(new File(directory+File.separator+"previous_versions"
                                +File.separator+destination+File.separator
                                +destination+"_"+previousversionformat.format(new Date())+user+".txt")))
                        {                        
                        }
                        else
                        {
                            logger.error("Unable to move file. File:"+afile.getAbsolutePath());
                        }
                    }*/
                    FileWriter f = new FileWriter(directory+File.separator+"previous_versions"
                                +File.separator+destination+File.separator
                                +destination+"_"+previousversionformat.format(new Date())+user+".txt");
                    PrintWriter out = new PrintWriter(f);
                    out.print(MessageAsText);
                    out.close();
                }

                try
                {
                    logger.info("Received "+destination);
                    FileWriter f = new FileWriter(directory+destination+".txt");
                    PrintWriter out = new PrintWriter(f);
                    out.print(MessageAsText);
                    out.close();
                }
                catch (Exception ex)
                {
                    logger.error("Unable to get write singleton. Ex="+ex.getMessage());
                }
            }

            catch (Exception ex)
            {
                logger.error("Unable to get write singleton. Ex="+ex.getMessage());
            }
            
        }
            
            catch (JMSException ex) {
                logger.error("Unable to decode message. Ex="+ex.getMessage(),ex);
            }
    }

    void setSingletonsWithoutHistory(LinkedHashMap<String, String> singletonsWithoutHistory)
    {
        singletonswithouthistory=singletonsWithoutHistory;
    }
    
}
