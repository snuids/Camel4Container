/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package helper;

import java.util.ArrayList;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple bean counting a number of messages received on a given period of time.<br/>
 * 
 * It returns the number of messages on the period in the header of the message using <br/>
 * <strong>OutputHeaderVariableName</strong> as header name. (Default value="Throughput")<br/>
 * 
 * 
 * @author Arnaud Marchand
 */
public class ThroughputProcessor implements Processor
{
    int integrationTime=5;
    String outputHeaderVariableName="Throughput";
    
    static final Logger logger = LoggerFactory.getLogger("ThroughputProcessor");
    ArrayList<Long> messagestimestamp=new ArrayList<Long>();

    public int getIntegrationTime()
    {
        return integrationTime;
    }

    public void setIntegrationTime(int integrationTime)
    {
        if(integrationTime<1)
            integrationTime=1;
        this.integrationTime = integrationTime;
    }

    public String getOutputHeaderVariableName()
    {
        return outputHeaderVariableName;
    }

    public void setOutputHeaderVariableName(String outputHeaderVariableName)
    {
        this.outputHeaderVariableName = outputHeaderVariableName;
    }
    
    
    
    @Override
    public void process(Exchange exchng) throws Exception
    {                
        messagestimestamp.add(System.currentTimeMillis());    
//        System.out.println("Size:"+messagestimestamp.size());    
    }
    
    public void readThroughput(Exchange exchng) throws Exception
    {
        Message in = exchng.getIn();
        
        in.setHeader(outputHeaderVariableName, 0);
        int i=0;
        long curtimestamp=System.currentTimeMillis()-(integrationTime* 1000L);
        
        for(i=0;i<messagestimestamp.size();i++)
        {
            if(messagestimestamp.get(i).longValue()>curtimestamp)
                break;
        }
        if((i>0)&&(i!=messagestimestamp.size()))
        {            
            messagestimestamp.subList(0, i).clear();
        }
        else if(i==messagestimestamp.size())
        {
            messagestimestamp.clear();
        }
        in.setHeader(outputHeaderVariableName, messagestimestamp.size());
    }
}
