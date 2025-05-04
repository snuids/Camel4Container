/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package helper;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.regex.Pattern;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * A bean that supervises a set of files defined by the parameter "filesDirectory"<br/>
 * using the file pattern "filePattern"<br/>
 * Once called, adds the header "defaultHeaderForResult" the content of the file<br/>
 * 
 * @author Arnaud Marchand
 */
public class FileSupervisor implements Processor
{
    static final Logger logger = LoggerFactory.getLogger("FileSupervisor");

    int         updateInterval=5000;
    String      defaultHeaderForResult="FileContent";    
    String      filesDirectory=".";
    Date        lastUpdate=new Date();
    String      defaultValue="";    
    boolean     tailFile=false;

    public boolean isTailFile()
    {
        return tailFile;
    }

    public void setTailFile(boolean tailFile)
    {
        this.tailFile = tailFile;
    }
    
    HashMap<String,SupervisedFile>     files=new HashMap<String,SupervisedFile>();
    
    

    public String getDefaultValue()
    {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue)
    {
        this.defaultValue = defaultValue;
    }
   
    
    
    public int getUpdateInterval()
    {
        return updateInterval;
    }

    public void setUpdateInterval(int updateInterval)
    {
        this.updateInterval = updateInterval;
    }

    public String getDefaultHeaderForResult()
    {
        return defaultHeaderForResult;
    }

    public void setDefaultHeaderForResult(String defaultHeaderForResult)
    {
        this.defaultHeaderForResult = defaultHeaderForResult;
    }

    public String getFilesDirectory()
    {
        return filesDirectory;
    }

    public void setFilesDirectory(String fileDirectory)
    {
        this.filesDirectory = fileDirectory;
    }

       
    public void getPropStringValue(Map headers,String aFileName,String aPath,String aDefaultValue,String aTargetHeader)
    {
        try
        {
            String res=updateFile(aFileName+".txt");
            String[] split=aPath.split("\\|");
            String res2="";
            for(int i=0;i<split.length-1;i++)
                res2+=split[i]+"|";
            //res2=res2.substring(0,res2.length()-1);
            res2=res2+"string="+split[split.length-1]+"=";
            
            int index=res.indexOf(res2);
            if(index<0)
            {
                headers.put(aTargetHeader, aDefaultValue);
            }
            else
            {
                int index2=res.indexOf('\r',index);
                int index3=res.indexOf('\n',index);
                
                if(index2>0)
                {
                    headers.put(aTargetHeader, res.substring(index+res2.length(),index2));
                }
                else if(index3>0)
                {
                    headers.put(aTargetHeader, res.substring(index+res2.length(),index3));
                }
                else
                    headers.put(aTargetHeader, res.substring(index+res2.length()));
                    
            }
        }
        catch(Exception e)
        {
            logger.equals("Unable to get prop value:"+aPath+" Ex="+e.getMessage());
        }
    }
    
    public void getPropIntValue(Map headers,String aFileName,String aPath,int aDefaultValue,String aTargetHeader)
    {
        try
        {
            String res=updateFile(aFileName+".txt");
            String[] split=aPath.split("\\|");
            String res2="";
            for(int i=0;i<split.length-1;i++)
                res2+=split[i]+"|";
            //res2=res2.substring(0,res2.length()-1);
            res2=res2+"int="+split[split.length-1]+"=";
            
            int index=res.indexOf(res2);
            if(index<0)
            {
                headers.put(aTargetHeader, aDefaultValue);
            }
            else
            {
                int index2=res.indexOf('\r',index);
                int index3=res.indexOf('\n',index);
                
                if(index2>0)
                {
                    headers.put(aTargetHeader, Integer.parseInt(res.substring(index+res2.length(),index2)));
                }
                else if(index3>0)
                {
                    headers.put(aTargetHeader, Integer.parseInt(res.substring(index+res2.length(),index3)));
                }
                else
                    headers.put(aTargetHeader, Integer.parseInt(res.substring(index+res2.length())));
                    
            }
        }
        catch(Exception e)
        {
            logger.equals("Unable to get prop value:"+aPath+" Ex="+e.getMessage());
        }
    }
    
    public void getPropBoolValue(Map headers,String aFileName,String aPath,boolean aDefaultValue,String aTargetHeader)
    {
        try
        {
            String res=updateFile(aFileName+".txt");
            String[] split=aPath.split("\\|");
            String res2="";
            for(int i=0;i<split.length-1;i++)
                res2+=split[i]+"|";
            //res2=res2.substring(0,res2.length()-1);
            res2=res2+"bool="+split[split.length-1]+"=";
            
            int index=res.indexOf(res2);
            if(index<0)
            {
                headers.put(aTargetHeader, aDefaultValue);
            }
            else
            {
                int index2=res.indexOf('\r',index);
                int index3=res.indexOf('\n',index);
                
                if(index2>0)
                {
                    headers.put(aTargetHeader, Boolean.parseBoolean(res.substring(index+res2.length(),index2)));
                }
                else if(index3>0)
                {
                    headers.put(aTargetHeader, Boolean.parseBoolean(res.substring(index+res2.length(),index3)));
                }
                else
                    headers.put(aTargetHeader, Boolean.parseBoolean(res.substring(index+res2.length())));
                    
            }
        }
        catch(Exception e)
        {
            logger.equals("Unable to get prop value:"+aPath+" Ex="+e.getMessage());
        }
    }
    
    public void process(Exchange exchng) throws Exception
    {
        String retvalue=defaultValue;
        Message in = exchng.getIn();
        String filename=(String)in.getHeader("FileName");
        
        synchronized(this)
        {            
            if(filename==null)
            {
                logger.error("No File Name Defined in File Supervisor.");
                return;    
            }
            
            try
            {
                retvalue=updateFile(filename);
                if(tailFile)
                {
                    if(files.containsKey(filename))
                    {
                        String updatedcontent=files.get(filename).updatedContent;
                        if((updatedcontent!=null)&&(updatedcontent.length()>0))                            
                            in.setBody(files.get(filename).updatedContent);
                        else
                            in.setBody("");
                        files.get(filename).updatedContent="";
                    }
                }
            }
            catch(Exception e)
            {
                logger.error(("Unable to read file content. Ex="+e.getMessage()));
            }
            
        }
   
        in.setHeader(defaultHeaderForResult, retvalue);
    }
    
    private void refreshFile(SupervisedFile aFile)
    {
        if(aFile.lastCheck.getTime()+updateInterval<new Date().getTime())
        {
            aFile.lastCheck=new Date();
            File file=new File(aFile.fileName);
            if(file.exists())
            {
                if(file.lastModified()!=aFile.lastModified)
                {
                    logger.info("File "+aFile.fileName+" modified.");
                    try
                    {
                        String oldcontent=aFile.content;
                        aFile.content=getFileContent(file);
                        aFile.lastModified=file.lastModified();
                        if(tailFile)
                        {
                            if(oldcontent.length()<aFile.content.length())
                            {
                                aFile.updatedContent=aFile.content.substring(oldcontent.length()-1);
                            }
                        }
                    }
                    catch (FileNotFoundException ex)
                    {
                        logger.info("Unable to refresh file. Ex="+ex.getMessage(),ex);
                    }
                    catch (IOException ex)
                    {
                        java.util.logging.Logger.getLogger(FileSupervisor.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }            
        }
    }
    
   
    
    private String getFileContent(File aFile) throws IOException
    {
        FileInputStream fis = new FileInputStream(aFile);
        byte[] data = new byte[(int)aFile.length()];
        fis.read(data);
        fis.close();
        //
        return new String(data, StandardCharsets.UTF_8);
    }

    private String updateFile(String filename) throws IOException
    {
        String retvalue="";

        File file=new File(filesDirectory+File.separator+filename);

        if(files.containsKey(filename))
        {
            try
            {
                SupervisedFile fil=files.get(filename);
                if(fil.lastDeleteCheck.getTime()+updateInterval<new Date().getTime())
                {
                    fil.lastDeleteCheck=new Date();
                    if(!file.exists())
                    {
                        logger.info(("Removing "+filename+" from cache."));
                        files.remove(filename);
                    }
                }
            }
            catch(Exception e)
            {
                logger.error("Unable to check if file must be removed. Ex="+e.getMessage());
            }
                        
        }
        
        if(files.containsKey(filename))
        {
            refreshFile(files.get(filename));
            retvalue=files.get(filename).content;
        }
        else
        {

            if(file.exists())
            {
                logger.info("First time for file:"+filename+" Adding it to cache. Path:"+file.getAbsolutePath());
                retvalue=getFileContent(file);
                
                //logger.info("File content:"+retvalue);
                SupervisedFile entry=new SupervisedFile(file.lastModified(),new Date(),retvalue,file.getAbsolutePath());
                if(tailFile)
                {
                    entry.updatedContent=entry.content;
                }
                files.put(filename, entry);
            }
            else
            {
                logger.info("Filename doest not exist. Returning default value. Path="+filesDirectory+File.separator+filename);
            }
        }
        return retvalue;
    }
}
