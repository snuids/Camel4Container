/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package helper;

import java.util.Locale;
import java.util.TimeZone;
import org.apache.velocity.tools.generic.DateTool;

/**
 * Used to convert dates in velocity reports<br/>
 * @author Arnaud Marchand
 */
public class DateUtilities
{
    DateTool    adatetool=new DateTool();
    Locale      enlocale = new Locale("en");
    TimeZone    utctimezone = TimeZone.getTimeZone("UTC");
        
    
    public DateTool dateTool()
    {
        return adatetool;
    }
    public Locale enLocale()
    {
        return enlocale;
    }
    public TimeZone utcTimeZone()
    {
        return utctimezone;
    }
}
