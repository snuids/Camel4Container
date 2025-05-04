/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package helper;

import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Arnaud Marchand
 */
public class CurrentDate
{
    static final Logger logger = LoggerFactory.getLogger("GetCurrentDate");

    
    public CurrentDate()
    {
        logger.info("GetCurrentDate initialized.");
    }
    
    public Date getCurrentDate()
    {
        return new Date();
    }
}
