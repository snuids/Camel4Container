/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package helper;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replaces \r\n by \n, splits per line and then
 * splits the body of the message using the specified delimiter.
 * Return an array of array of fields. X[lines][cols]
 * 
 * @author Arnaud Marchand
 */

public class Splitter implements Processor
{
    String delimiter="\t";
    static final Logger logger = LoggerFactory.getLogger("Splitter");
    
    public String getDelimiter()
    {
        return delimiter;
    }

    public void setDelimiter(String delimiter)
    {
        this.delimiter = delimiter;
    }
        

    @Override
    public void process(Exchange exchng) throws Exception
    {
        
        Message in = exchng.getIn();
        String alllines=in.getBody(String.class).replaceAll("\r\n", "\n");
        //logger.info("All lines:"+alllines);
        String []lines=alllines.split("\n");  
        String [][]tables=new String[lines.length][];
        int start=0;
        for(int i=0;i<lines.length;i++)
        {
            if(lines[i].length()>1)
            {
                tables[start]=lines[i].split(delimiter,-1);
                //logger.info("Line ["+i+"]: Line:"+lines[i]+ " Length:"+tables[start].length);
                start++;
            }
        }
        in.setBody(tables);
    }
    
}
