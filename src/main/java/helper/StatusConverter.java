/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package helper;

import java.util.Map;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated
 * @author Arnaud Marchand
 */
public class StatusConverter implements Processor
{
    static final Logger logger = LoggerFactory.getLogger("StatusConverter");
    String              Destination="";
    
    public StatusConverter()
    {
        logger.info("Status converter initialized.");
    }
    
    public void setDestination(String Destination)
    {
       logger.info("Destination set to:"+Destination);
       this.Destination=Destination;
       
    }
    
    @Override
    public void process(Exchange exchng) throws Exception
    {
        logger.info("Converting status.");
        String []split=((String)exchng.getIn().getBody()).split("\\|");
        logger.info("Len="+split.length+" >"+ exchng.getIn().getBody());
        
        String endpointstr=Destination;
        logger.info("Dispatch to:"+endpointstr);
        org.apache.camel.Endpoint endp=exchng.getContext().getEndpoint(endpointstr);

        Exchange exchange2 = endp.createExchange();
        StringBuffer buf=new StringBuffer();
        Map<String,Object> res=exchange2.getIn().getHeaders();
        
        res.put("BSProcess",split[0]);
        res.put("BSGroup",split[1]);
        res.put("BSServer",split[2]);
        res.put("BSHost",split[3]);
        res.put("BSVersion",split[5]);
        res.put("BSComVersion",split[4]);
        res.put("BSMemory",split[6]);
        
        String[] lastres=split[7].split(",");
        
        for(String str:lastres)
        {
            String []cols=str.split("=");
            if(cols.length>1)
            {
                if(!res.containsKey(cols[0]))
                    res.put(cols[0], cols[1]);
            }
            buf.append(str+"\r\n");
            
            
        }
        exchange2.getIn().setBody(buf.toString());
        endp.createProducer().process(exchange2);
    }
}
