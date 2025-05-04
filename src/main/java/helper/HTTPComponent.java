/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package helper;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author snuids
 */
public class HTTPComponent implements Processor
{
    static final Logger logger = LoggerFactory.getLogger("HTTPComponent");

    public void process(Exchange exchange) throws Exception
    {
        Message in = exchange.getIn();
        String alllines=in.getBody(String.class);
        long startTime = System.nanoTime(); 
        logger.info("===> Thread:"+Thread.currentThread().getName()+" HTTP:"+alllines);
        
        try
        { 
        //URL yahoo = new URL("http://www.yahoo.com/");
            URL urltoload = new URL(alllines);
            URLConnection yc = urltoload.openConnection();
            BufferedReader in2 = new BufferedReader(
                                    new InputStreamReader(
                                    yc.getInputStream()));
            String inputLine;
            StringBuilder buildstr=new StringBuilder();

            while ((inputLine = in2.readLine()) != null) 
                buildstr.append(inputLine);
                //System.out.println(inputLine);
                in2.close();
            exchange.getOut().setBody(buildstr.toString());
        }
        catch(Exception e)
        {
            logger.error("==> Thread:"+Thread.currentThread().getName()+" Unabe to load http.");
        }
        
        long estimatedTime = System.nanoTime() - startTime;
        System.out.println("==> Thread:"+Thread.currentThread().getName()+" Time:"+(estimatedTime/1000000));
        logger.info("==> Thread:"+Thread.currentThread().getName()+" Time:"+(estimatedTime/1000000));
        
    }

    
}
