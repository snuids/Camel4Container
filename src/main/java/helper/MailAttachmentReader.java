/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package helper;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.activation.DataHandler;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.camel.attachment.AttachmentMessage;

/**
 * Dispatches mail attachments depending on their types
 * @author Arnaud Marchand
 */
public class MailAttachmentReader implements Processor
{
    static final Logger logger = LoggerFactory.getLogger("MailAttachmentReader");
    
    Map dispatchers=new LinkedHashMap();
    
    public MailAttachmentReader()
    {
        logger.info("Mail attachement reader created.");
    }

    public void setDispatchers(Map dispatchers)
    {
        this.dispatchers=dispatchers;
    }
    
    
    @Override
    public void process(Exchange exchange) throws Exception
    {
        AttachmentMessage attachmentsMessage = (AttachmentMessage) exchange.getIn();
        
        Map<String, DataHandler> attachments = attachmentsMessage.getAttachments();
        
        if (!attachments.isEmpty()) 
        {
            for (String name : attachments.keySet()) 
            {
                try
                {
                    DataHandler dh = attachments.get(name);
                    // get the file name
                    String filename = dh.getName();

                    // get the content and convert it to byte[]
                    byte[] data = exchange.getContext().getTypeConverter()
                                      .convertTo(byte[].class, dh.getInputStream());

                    //org.apache.camel.Endpoint endp=exchange.getContext().getEndpoint("jms:queue:aaaaaa");

                    String []split=filename.split("\\.");

                    if(!dispatchers.containsKey(split[split.length-1]))
                    {
                        logger.info("No dispatcher defined for attachements:"+split[split.length-1]+" file:"+filename);
                        return;
                    }
                    if(filename.indexOf("gif")>=0)
                    {
                        logger.info("Gif found");
                        
                    }

                    String endpointstr=(String)dispatchers.get(split[split.length-1]);
                    logger.info("Dispatch attachement file:"+filename+" to:"+endpointstr);
                    org.apache.camel.Endpoint endp=exchange.getContext().getEndpoint(endpointstr);

                    Exchange exchange2 = endp.createExchange();
                    exchange2.getIn().setBody(data);
                    exchange2.getIn().setHeader("FileName", filename);
                    exchange2.getIn().setHeader("CamelFileName", filename);
                    if(exchange.getIn().getHeader("From")!=null)
                    {
                        exchange2.getIn().setHeader("From", exchange.getIn().getHeader("From"));
                    }
                    if(exchange.getIn().getHeader("To")!=null)
                    {
                        exchange2.getIn().setHeader("To", exchange.getIn().getHeader("To"));
                    }


                    endp.createProducer().process(exchange2);
                }
                catch(Exception e)
                {
                    logger.error("ERROR:Unable to forward attachment. Ex="+e.getMessage(),e);
                }
                // write the data to a file
                //FileOutputStream out = new FileOutputStream(target
                //            +System.getProperty("file.separator")+ filename);
                //out.write(data);
                //out.flush();
                //out.close();
            }
        }
    }
    
    
}
