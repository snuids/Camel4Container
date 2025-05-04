/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package helper;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author snuids
 */
public class HTTPComponent1 implements Processor
{
    static final Logger logger = LoggerFactory.getLogger("HTTPComponent");

    public void process(Exchange exchange) throws Exception
    {
        Message in = exchange.getIn();
        
        ArrayList<String> test=new ArrayList<String>();
        
        for(int i=0;i<10;i++)
        {
            test.add((">"+i+":CSSDODIUIOSUDIOSIOUDIOUSIODUIOSUIODUIOSUIODUSUDIOIUSQSHQKJDHJKSJKDHJKSHDJKHSJKHDJKHSKJHDJKSHJKDHSJKHDKJHSKJDHKJSHKDJSKJ"));
        }
        exchange.getOut().setBody(test);
        
    }

    public List<Object> splitBody(Object body)
    {
        return (List<Object>)body;
    }
}
