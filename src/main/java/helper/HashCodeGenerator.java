/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package helper;

import java.util.Date;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates a hashcode from a string using a 64 bits hashcode function .
 * The source string is passed in the header HashCodeSource and the result passed in HashCodeTarget as long.
 * @author Arnaud Marchand
 */
public class HashCodeGenerator implements Processor
{
    static final Logger logger = LoggerFactory.getLogger("HashCodeGenerator");

               
    public HashCodeGenerator()
    {
        logger.info("HashCode Generator initialized.");
    }

    public void process(Exchange exchange) throws Exception
    {
        String retvalue="NA";
        Message in = exchange.getIn();
        String stringin=(String)in.getHeader("HashCodeSource");
        
        long hash=7;
        for (int i=0; i < stringin.length(); i++) {
            hash = hash*31+stringin.charAt(i);
        }
        
        if(hash<0)
            hash=hash*(-1);
        
        in.setHeader("HashCodeTarget", hash);
    }
    
    
}
