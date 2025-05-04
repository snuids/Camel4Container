/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package helper;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
//import org.apache.camel.component.ehcache.EhcacheConstants;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Can accept a string, an array of string, or an array of array of string<br/>
 * Scans all the records and transform the fields starting with #CONVERTDATE#<br/>
 * using the following logic:<br/><br/>
 * <strong>#CONVERTDATE#ORIGNALSTRING#ORIGINALFORMAT#NEWFORMAT#CONVERTTYPE</strong><br/>
 * <strong>CONVERTTYPE</strong> can be empty or equal to TOUTC<br/>
 * <br/>
 * so the field:<strong>#CONVERTDATE#2013-09-0411:20:00#yyyyMMMdddHH:mm:ss#yyyyMMddd HH:mm:ss#</strong><br/>
 * becomes <strong>2013SEP04 11:20:00</strong><br/>
 * 
 * 
 * @author Arnaud Marchand
 */

public class DateConverter implements Processor
{   
    static final Logger logger = LoggerFactory.getLogger("DateConverter");
    boolean throwErrorInCaseOfParsingError=true;

    @Override
    public void process(Exchange exchng) throws Exception
    {
        
        Message in = exchng.getIn();
        Object body=in.getBody();
        
        if(body instanceof String[][])
        {
            String[][] fields=(String[][])body;
            for(int i=0;i<fields.length;i++)
            {
                for(int j=0;j<fields[i].length;j++)
                {
                    fields[i][j]=Convert(fields[i][j]);
                }
            }            
            //logger.info("Done");
        }
        else if(body instanceof String[])
        {
            String[] fields=(String[])body;
            for(int i=0;i<fields.length;i++)
            {
                fields[i]=Convert(fields[i]);
            }
        }
        else
        {
            in.setBody(Convert((String)body));
        }
        
    }

    public boolean isThrowErrorInCaseOfParsingError()
    {
        return throwErrorInCaseOfParsingError;
    }

    public void setThrowErrorInCaseOfParsingError(boolean throwErrorInCaseOfParsingError)
    {
        this.throwErrorInCaseOfParsingError = throwErrorInCaseOfParsingError;
    }
    
    
    
    String Convert(String inStr) throws ParseException
    {
        
        try
        {
            String[] cols=inStr.split("#");
            if(cols.length<5)
                return inStr;
            
            SimpleDateFormat format=new SimpleDateFormat(cols[3]);
            Date res=format.parse(cols[2]);
            
            if((cols.length>5)&&(cols[5].indexOf("TOUTC")>=0))
            {
                System.out.println("Local Date : " + res);
                DateTimeZone tz = DateTimeZone.getDefault();
                Date utcDate = new Date(tz.convertLocalToUTC(res.getTime(), false));
                System.out.println("UTC Date : " + utcDate);
                res=utcDate;
            }
            
            SimpleDateFormat format2=new SimpleDateFormat(cols[4]);
            return format2.format(res);
        }
        catch (ParseException ex)
        {
            logger.error("Unable to parse date. Ex="+ex.getMessage());
            if(!throwErrorInCaseOfParsingError)
                return inStr;
            else
                throw ex;
        }
    }
}
