/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package helper;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Add to the processed message the following headers:<br/> 
 * <ul>
 * <li>COMPUTERNAME</li><br/>
 * <li>MEMORY</li><br/>
 * <li>JAVAVERSION</li><br/>
 * <li>CAMELVERSION</li><br/>
 * <li>LAUNCHTIME</li><br/>
 * </ul>
 * @author Arnaud Marchand
 */
public class ProcessInfo implements Processor
{
    static final Runtime runtime = Runtime.getRuntime();
    static final Logger logger = LoggerFactory.getLogger("ProcessInfo");
    String computerName="";
    String javaVersion="";
    String launchTime="";
    SimpleDateFormat formatlaunchtime = new SimpleDateFormat("MM/dd/yyyy hh:mm a");
    
    @Override
    public void process(Exchange exchng) throws Exception
    {
        Message in = exchng.getIn();
        if(computerName.length()==0)
        {
            try
            {
                computerName = InetAddress.getLocalHost().getHostName();
            }
            catch (UnknownHostException ex)
            {
                computerName = "NA";
            }
        }
        if(javaVersion.length()==0)
        {
            try
            {
                javaVersion = System.getProperty("java.version");
            }
            catch (Exception ex)
            {
                javaVersion = "NA";
            }
        }
        if(launchTime.length()==0)
        {
            launchTime=formatlaunchtime.format(new Date());
        }
                
        in.setHeader("COMPUTERNAME", computerName);
        in.setHeader("MEMORY", ((runtime.totalMemory() - runtime.freeMemory()) / 1000000));
        in.setHeader("JAVAVERSION", javaVersion);
        in.setHeader("CAMELVERSION", camelworker.CamelWorker.version);
        in.setHeader("LAUNCHTIME", launchTime);
    }
    
}
