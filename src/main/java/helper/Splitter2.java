/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package helper;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Andrew Filipowski
 */
public class Splitter2 implements Processor {

    static final Logger logger = LoggerFactory.getLogger("Splitter2");
    CamelContext context;
    private String destination;
    private String split;
    private String ErrorDestination;
    
    /**
     * @return the destination
     */
    public String getDestination() {
        return destination;
    }

    /**
     * @param destination the destination to set
     */
    public void setDestination(String destination) {
        this.destination = destination;
    }
    /**
     * @return the split
     */
    public String getSplit() {
        return split;
    }

    /**
     * @param split the split to set
     */
    public void setSplit(String split) {
        this.split = split;
    }

    @Override
    public void process(Exchange exchng) throws Exception {
        try
        {
        String strBody = exchng.getIn().getBody().toString().replace("\r", "").replace("\n", "");
        context = exchng.getContext();
        org.apache.camel.Endpoint endp = context.getEndpoint(getDestination());
        String[] stArr = strBody.split(split);
         System.out.println("body on :" + strBody);
         logger.debug("body on :" + strBody);
        logger.debug("splitting on :" + split);
                 System.out.println("splitting on :" + split);
         logger.debug("array length on :" + stArr.length);
                  System.out.println("array length on :" + stArr.length);
        Exchange exchange2 = endp.createExchange();
        exchange2.getIn().setBody(stArr);
        exchange2.getIn().setHeader("Generation time : ", "");

        endp.createProducer().process(exchange2);
        }
              catch (Exception e) {
            try {
                org.apache.camel.Endpoint endp = context.getEndpoint(getErrorDestination());
                Exchange exchange2 = endp.createExchange();
                exchange2.getIn().setBody("ALERT" + "\r\n"
                        + ".M/An error occured in Splitter (APSB). " + e + "|| original message :"  + exchng.getIn().getBody().toString() + "\r\n"
                        + ".P/10"+ "\r\n"
                        + ".G/APSB"+ "\r\n"
                        + ".T/CRASH"+ "\r\n"                      
                        + "ENDALERT");
                endp.createProducer().process(exchange2);
            } catch (Exception e2) {
                System.out.println(e2.getMessage());
            }
        }

    }

    /**
     * @return the ErrorDestination
     */
    public String getErrorDestination() {
        return ErrorDestination;
    }

    /**
     * @param ErrorDestination the ErrorDestination to set
     */
    public void setErrorDestination(String ErrorDestination) {
        this.ErrorDestination = ErrorDestination;
    }
}
