/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pubsub.website;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Arnaud Marchand
 */
class OneSecondRecords
{
    static final Logger logger = LoggerFactory.getLogger("HttpFileServer");
    
    public long CurrentSecond;
    public HashMap<String,List<OneMessage>> Records=new HashMap<String, List<OneMessage>>();
    Pattern patternBSUSER = Pattern.compile("Password:.*");
    Pattern patternREQUEST = Pattern.compile("\\/[^\\\\/]*\\/?$");
    Pattern patternREPLY = Pattern.compile("[\\w]*\\/([\\w])*$");
    

    public OneSecondRecords(long CurrentSecond)
    {
        this.CurrentSecond = CurrentSecond;
    }
    
    public void AddOneValue(String aTopic,String aPubSubID,String aClientName,String aMessage)
    {
        try
        {
            if(!Records.containsKey(aTopic))
            {
                Records.put(aTopic, new ArrayList<OneMessage>());
            }
            List<OneMessage> mess=Records.get(aTopic);
            if(aTopic.compareTo("ROUTESTATUS")==0) {
                mess.add(new OneMessage(aPubSubID, aClientName, "...MesLen="+aMessage.length()));
            } else if (aTopic.compareTo("AUTHREQUEST")==0) {
                aMessage = patternREQUEST.matcher(aMessage).replaceAll("/********/");
                mess.add(new OneMessage(aPubSubID,aClientName, aMessage));
            } else if (aTopic.compareTo("AUTHREPLY")==0) {
                aMessage = patternREPLY.matcher(aMessage).replaceAll("*********/$1");
                mess.add(new OneMessage(aPubSubID,aClientName, aMessage));
            } else if (aTopic.compareTo("BSUSER")==0) {
                aMessage = patternBSUSER.matcher(aMessage).replaceAll("Password:********");
                mess.add(new OneMessage(aPubSubID,aClientName, aMessage));
            } else {
                mess.add(new OneMessage(aPubSubID,aClientName, aMessage));
            }
        }
        catch(Exception e)
        {
            logger.error("Unable to add new message, error is" + e.getMessage());
        }
    }
}
