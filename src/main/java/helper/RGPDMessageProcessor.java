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
 * @author Arnaud Marchand
 */


public class RGPDMessageProcessor implements Processor
{    
    static final Logger logger = LoggerFactory.getLogger("RGPDMessageProcessor");
    
    @Override
    public void process(Exchange exchng) throws Exception
    {
        try
        {      
            String body=(String)exchng.getIn().getBody();            
            String res=cleanMessageForRGPD(body);
            
            exchng.getOut().setBody(res);
        }
        catch(Exception e)
        {
            logger.error("Unable to clean message:"+ exchng.getIn().getBody() +" Ex="+e.getMessage(),e);
            throw e;
        }
    }
    public String cleanMessageForRGPD(String aMessage)
    {
        String separator="\n";
                
        if (aMessage.indexOf("\r\n")>=0)
        {
            separator="\r\n";
        }
        else if(aMessage.indexOf("\r")>=0){
            separator="\r";
        }
        
        StringBuilder buf=new StringBuilder();
        
        for(String line:aMessage.split(separator))
        {
            if (line.startsWith(".P/"))
            {
                StringBuilder newp=new StringBuilder();
                int index=0;
                for(String col: line.split("/"))
                {
                    if(index==0)
                    {
                        newp.append(col).append("/");
                    }
                    else
                    {                        
                        String hash=Character.toString ((char) (index+64))+col.hashCode();
                        hash=hash.replace("-","M");
                        newp.append(hash).append("/");
                    }
                    index++;
                }
                buf.append(newp).append(separator);
            }
            else if (line.startsWith(".L/"))
            {
                StringBuilder newp=new StringBuilder();
                int index=0;
                for(String col: line.split("/"))
                {
                    if(index==0)
                    {
                        newp.append(col).append("/");
                    }
                    else
                    {                        
                        String hash=Character.toString ((char) (index+64))+col.hashCode();
                        hash=hash.replace("-","M");
                        newp.append(hash).append("/");
                    }
                    index++;
                }
                buf.append(newp).append(separator);
            }
            else
                buf.append(line).append(separator);
        }
        return buf.toString();
    }
}
