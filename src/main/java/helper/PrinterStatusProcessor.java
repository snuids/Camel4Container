/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package helper;

import java.util.HashMap;
import java.util.Map;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts a WEBSTATUS such as:<br/>
 * <br/>
 * <strong>A|2|CI001|0|635120620680273437|::1|/WebClients/Stations/Login.aspx|VERSION=2.16.0.0,PRINTER_IERBAG-CI001.Alive=0,PRINTER_IERBAG-CI001.PaperOut=0,PRINTER_IERBAG-CI001.Busy=0,PRINTER_IERBAG-CI001.CutterFault=0,PRINTER_IERBAG-CI001.LabelPrinted=0,PRINTER_IERBAG-CI001.LastStatus=,PRINTER_IERBAG-CI001.LabelWaiting=0,PRINTER_IERPAX-CI001.Alive=0,PRINTER_IERPAX-CI001.PaperOut=0,PRINTER_IERPAX-CI001.Busy=0,PRINTER_IERPAX-CI001.CutterFault=0,PRINTER_IERPAX-CI001.LabelPrinted=0,PRINTER_IERPAX-CI001.LastStatus=,PRINTER_IERPAX-CI001.LabelWaiting=0|Ready|NotIdentified|||</strong><br/>
 * <br/>
 * in a one printer per line format<br/>
 * <strong>CI001,IERBAG-CI001,1,X,X,</strong><br/>
 * <strong>CI001,IERPAX-CI001,1,0,0,</strong><br/>
 * <br/>
 * Station,Printer,Feed1,Feed2,Feed3<br/>
 * With X for non present,O(not zero) for empty,1 for Ok<br/>
 * 
 * @author Arnaud Marchand
 */
public class PrinterStatusProcessor implements Processor
{
    static final Logger logger = LoggerFactory.getLogger("PrinterStatusProcessor");
    
    public PrinterStatusProcessor()
    {
        logger.info("Printer Status Processor initialized.");
    }
    
    
    @Override
    public void process(Exchange exchng) throws Exception
    {
        //logger.info("Converting printer status.");
        try
        {      
            StringBuffer out=new StringBuffer();
            
            String []lines=((String)exchng.getIn().getBody()).replace("\r", "").split("\n");
            for(String line:lines)
            {
                String []split=line.split("\\|");
                //logger.info("Len="+split.length+" >"+line);           

                String station=split[2];
                String printers=split[7];

                String []cols=printers.split(",");
                HashMap<String,String> vals=new HashMap<String,String>();
                HashMap<String,String> printerpresents=new HashMap<String,String>();

                for(String str:cols)
                {
                    String[] cols2=str.split("=");
                    if(cols2.length>1)
                    {
                        if(cols2[0].startsWith("PRINTER_"))
                        {
                            String printername=cols2[0].substring(8).split("\\.")[0];
                            if(!printerpresents.containsKey(printername))
                                printerpresents.put(printername, printername);
                        }
                        vals.put(cols2[0],cols2[1]);
                    }
                }
                // create a line per printer
                for(String str:printerpresents.keySet())
                {
                    String printerstatus="";
                    printerstatus=vals.get("PRINTER_"+str+".LastStatus");
                    if(printerstatus==null)
                        printerstatus="";
                    int index=printerstatus.indexOf("OK");
                    String feeder1="X";
                    String feeder2="X";
                    String feeder3="X";
                    if(index>=0)
                    {
                        String feederstate=printerstatus.substring(index+2);
                        char[] feeders=feederstate.toCharArray();
                        if(feeders.length>0)
                            feeder1=""+feeders[0];
                        if(feeders.length>1)
                            feeder2=""+feeders[1];
                        if(feeders.length>2)
                            feeder3=""+feeders[2];
                    }
                    
                    out.append(station).append(",")
                            .append(str).append(",")   
                            .append(feeder1).append(",")   
                            .append(feeder2).append(",")   
                            .append(feeder3).append(",")   
                    .append("\n");
                }
            }
            exchng.getIn().setBody(out.toString());
        }
        catch(Exception e)
        {
            logger.error("Unable to handle message:"+ exchng.getIn().getBody() +" Ex="+e.getMessage(),e);
        }
        
    }
}
