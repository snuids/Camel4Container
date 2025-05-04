/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package helper;

import java.util.Date;

/**
 *
 * @author snuids
 */
public class SupervisedFile
{
    long    lastModified;
    Date    lastCheck;
    Date    lastDeleteCheck;
    String  content;
    String  fileName;
    String  updatedContent="";

    public SupervisedFile(long lastModified, Date lastCheck, String content, String fileName)
    {
        this.lastModified = lastModified;
        this.lastCheck = lastCheck;
        this.lastDeleteCheck=lastCheck;
        this.content = content;
        this.fileName = fileName;
    }
    
    
}
