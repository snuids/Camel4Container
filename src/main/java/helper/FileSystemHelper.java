/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package helper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.logging.Level;
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

public class FileSystemHelper implements Processor
{
    static final Logger logger = LoggerFactory.getLogger("LogHelper");
    
    public String rootDirectory="";

    public String getRootDirectory()
    {
        return rootDirectory;
    }

    public void setRootDirectory(String rootDirectory)
    {
        logger.info("Root Directory set to "+rootDirectory+"...");
        this.rootDirectory = rootDirectory;
    }

    @Override
    public void process(Exchange exchng) throws Exception
    {        
        exchng.getIn().setHeader("Logger", logger);
    }
    
    public String getFolderContent(String dir)
    {
        File[] files = new File(rootDirectory+File.separatorChar+dir).listFiles();
        if(files==null)
            return "[]";
        ObjectMapper mapper = new ObjectMapper();
        
        ArrayList<OneFile> flist=new ArrayList<OneFile>();
        
        for(File f:files)
        {
            flist.add(new OneFile(f));
        }
        
        try
        {
            return mapper.writeValueAsString(flist);
        }
        catch (JsonProcessingException ex)
        {
            logger.error("Unable to convert string to JSON. Ex="+ex.getMessage());
        }
        return "{error:-1}";
    }
    
    //public String readLastBytesFromFile(string aFileName)
    public String getFolderHierachy()
    {
        logger.info("Get Folder Hierarchy of:"+rootDirectory);
        ArrayList<String> files=new ArrayList<String>(); 
        
        getFolderHierachy(""+File.separatorChar,files);
        ObjectMapper mapper = new ObjectMapper();
        try
        {
            return mapper.writeValueAsString(files);
        }
        catch (JsonProcessingException ex)
        {
            logger.error("Unable to convert string to JSON. Ex="+ex.getMessage());
        }
        return "{error:-1}";
    }

    private void getFolderHierachy(String curDir, ArrayList<String> files)
    {        
        curDir=curDir.replace("___", "/");
        File[] directories = new File(rootDirectory+File.separatorChar+curDir).listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.isDirectory();
            }
        });
        if(directories!=null)
        {
            for(File d:directories)
            {
                files.add(curDir+d.getName());
                getFolderHierachy(curDir+d.getName()+File.separatorChar,files);
            }
        }
        //logger.info("Files:"+files.size());
        if(files.size()>10000)
        {
            logger.error("Too many directories in LogHelper...");
        }
    }
    
    public String readLastBytesOfFile(String aFile,int numberOfBytes)
    {
        try
        {
            aFile=aFile.replace("___", "/");
            File file = new File(rootDirectory+File.separatorChar+ aFile);
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            
            // Seek to the end of file
            if(numberOfBytes>file.length())
                numberOfBytes=(int)file.length();
            raf.seek(file.length() - numberOfBytes);
// Read it out.
            byte[] bytes=new byte[numberOfBytes];
            raf.read(bytes, 0, numberOfBytes);
            return new String(bytes); 
        }
        catch (Exception ex)
        {
            logger.error("Unable to read file. Ex="+ex.getMessage());
            return "Unable to read file. Ex="+ex.getMessage();
            
        }
    }
    
    
    public class OneFile
    {
        public String name;
        public boolean dir;
        public long fileSize=0;
        public long lastModified;
        public long creationTime;
        
        public OneFile(File inFile)
        {
            name=inFile.getName();
            dir=inFile.isDirectory();
            
            fileSize=inFile.length();
            
            BasicFileAttributes attr;
            try
            {
                attr = Files.readAttributes(inFile.toPath(), BasicFileAttributes.class);
                lastModified=attr.lastModifiedTime().toInstant().toEpochMilli();
                creationTime=attr.creationTime().toInstant().toEpochMilli();
            }
            catch (IOException ex)
            {
                logger.error("Unable to read file attributes. Ex="+ex.getMessage());
            }

            
        }
    }
}
