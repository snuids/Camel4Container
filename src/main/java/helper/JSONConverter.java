/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package helper;

import com.mongodb.BasicDBObject;
import java.util.ArrayList;
import java.util.Date;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts a text body to a JSONObject<br/>
 * Use new Date() to insert current date<br/>
 * @author Arnaud Marchand
 */
public class JSONConverter implements Processor
{
    JSONParser parser=new JSONParser(net.minidev.json.parser.JSONParser.USE_INTEGER_STORAGE);
                
    @Override
    public void process(Exchange exchng) throws Exception
    {
        
        Message in = exchng.getIn();
        String alllines=in.getBody(String.class);
        //logger.info("All lines:"+alllines);
        
        alllines=alllines.replaceAll("new Date\\(\\)", "\"REPLACEDATE()\"");
            
        JSONObject parsed;
        synchronized(parser)
        {
             parsed= (JSONObject) parser.parse(alllines);
        }
        ArrayList<String> keystoreplace=new ArrayList<String>();
        for(String key:parsed.keySet())
        {                
            Object objres=parsed.get(key);
            if((objres instanceof String)&&(((String)objres).compareTo("REPLACEDATE()")==0))
            {
                keystoreplace.add(key);
            }

        }
        for(String key:keystoreplace)
        {
            parsed.remove(key);
        }
        BasicDBObject obj=new com.mongodb.BasicDBObject(parsed);
        for(String key:keystoreplace)
        {
            obj.put(key, new Date());
        }

        in.setBody(obj);
    }
    
}
