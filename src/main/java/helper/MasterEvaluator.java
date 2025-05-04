/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package helper;

import java.util.Date;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Must be fed with MAS and or STATUS messages:<br/>
 * <br/>
 * Adds the header IsMaster with the integer value 1 or 0 depending on the master state.<br/>
 * Adds the header IsMaster with the integer value 1 or 0 depending on the local master state.<br/>
 * @author Arnaud Marchand
 */
public class MasterEvaluator implements Processor
{
    static final Logger logger = LoggerFactory.getLogger("MasterEvaluator");

    String              CurrentServerID="-1";
    int                 LastMasterValue=0;
    boolean             localMaster=false;
    Date                lmLocalMasterSince=new Date();
    Date                lmLastOtherLocalMasterDate=new Date();
    String              localMasterProcessName="";

    public String getLocalMasterProcessName()
    {
        return localMasterProcessName;
    }

    public void setLocalMasterProcessName(String localMasterProcessName)
    {
        this.localMasterProcessName = localMasterProcessName;
    }

    
    
    public String getCurrentServerID()
    {
        return CurrentServerID;
    }

    public void setCurrentServerID(String CurrentServerID)
    {
        logger.info("CurrentServerID set to:"+CurrentServerID);
        this.CurrentServerID = CurrentServerID;
    }
    
    
    
    public MasterEvaluator()
    {
        logger.info("Printer Status Processor initialized.");
    }
    
    
    @Override
    public void process(Exchange exchng) throws Exception
    {        
        // Add local master info
        String statusvariables = "LOCALMASTER=" + (localMaster ? 1 : 0);
        if(localMaster)
            statusvariables += ",LOCALMASTERSINCE=" + lmLocalMasterSince.getTime();
        statusvariables+=",";

        try
        {    
            if(exchng.getIn().getBody() instanceof String)
            {
                String body=(String)exchng.getIn().getBody();
                if((body!=null)&&(body.startsWith("MASTER|")))
                {
                    if(body.startsWith("MASTER|"+CurrentServerID))
                        LastMasterValue=1;
                    else
                        LastMasterValue=0;
                }
                try
                {
                    if((body!=null)&&(body.startsWith(localMasterProcessName+"|")))
                    {
                        String[] cols = body.split("\\|");
                        if(cols[2].compareTo(CurrentServerID)!=0)
                        {
                            boolean otherismaster = (body.indexOf("LOCALMASTER=1")>=0);
                            if (otherismaster)
                            {
                                int ind1 = body.indexOf("LOCALMASTERSINCE=");
                                lmLastOtherLocalMasterDate = new Date();
                                ind1 = ind1 + "LOCALMASTERSINCE=".length();
                                if (ind1 > 0)
                                {
                                    int ind2 = body.indexOf(",", ind1);
                                    if (ind2 > 0)
                                    {
                                        String lmd = body.substring(ind1, ind2);
                                        Date otherlmdate = new Date(Long.parseLong(lmd));
                                        logger.debug("Received status from other server. Date:" + otherlmdate.getTime());
                                        //theLogger.Debug("DIFF=" + (DateTime.UtcNow - otherlmdate).TotalSeconds);
                                        if (((otherlmdate.getTime() < lmLocalMasterSince.getTime()))
                                            ||((otherlmdate.getTime() == lmLocalMasterSince.getTime())
                                                &&(CurrentServerID.compareTo("1")!=0)))

                                        {
                                            if (localMaster)
                                            {
                                                logger.info("Becoming local slave.");
                                                localMaster = false;
                                            }
                                        }
                                    }
                                }
                                boolean otherrefusemaster = (body.indexOf("REFUSEMASTER=1")>=0);
                                if (otherrefusemaster)
                                {
                                    logger.debug("Refuse master received from other process.");                            
                                }
                            }

                        }
                    }

                    if(!localMaster)
                    {
                        if (lmLastOtherLocalMasterDate.getTime()+(1000*10)<System.currentTimeMillis())
                        {
                                localMaster=true;
                                lmLocalMasterSince = new Date();
                                logger.info("Becoming local master.");      
                        }
                    }
                }
                catch(Exception elm)
                {
                    logger.error("Unable to compute local master. Ex="+elm.getMessage(),elm);
                }
            }
            exchng.getIn().setHeader("LocalMasterVariables", statusvariables);
            exchng.getIn().setHeader("IsMaster", LastMasterValue);
            exchng.getIn().setHeader("IsLocalMaster", (localMaster ? 1 : 0));
        }
        catch(Exception e)
        {
            logger.error("Unable to handle master message.",e);
        }
        
    }

}
