/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.equans.pubsub.utils;

import java.util.HashMap;

/**
 *
 * @author Carnus
 */
public class PropParser {
    
    private static void DeserializeOneLine(String theline, HashMap res) throws Exception
    {
        String[] splitequal = theline.split("=");
        String[] splitpipe = splitequal[0].split("\\|");
        if (splitpipe.length == 1)
        {
            int index1 = theline.indexOf("=");
            int index2 = theline.indexOf("=", index1 + 1);
           
            if (splitpipe[0].equals("string")) {
               
                res.put(splitequal[1], theline.substring(index2 + 1));
            } else if (splitpipe[0].equals("float")) {
                try
                {
                    res.put(splitequal[1], new Float(theline.substring(index2 + 1)));
                }
                catch (NumberFormatException eeee)
                {
                    throw new Exception("Unable to decode float field=" + splitequal[1] + " value=" +theline.substring(index2 + 1));
                }
               
            } else if (splitpipe[0].equals("long")) {
                try
                {
                    res.put(splitequal[1], new Long(theline.substring(index2 + 1)));
                }
                catch (NumberFormatException eeee)
                {
                    throw new Exception("Unable to decode long field=" + splitequal[1] + " value=" + theline.substring(index2 + 1));
                }
               
            } else if (splitpipe[0].equals("int")) {
                try
                {
                    res.put(splitequal[1], new Integer(theline.substring(index2 + 1)));
                }
                catch (NumberFormatException eeee)
                {
                    throw new Exception("Unable to decode int field=" + splitequal[1] + " value=" +theline.substring(index2 + 1));
                }
               
            } else if (splitpipe[0].equals("bool")) {
                res.put(splitequal[1], Boolean.parseBoolean(theline.substring(index2 + 1)));
            }
        }
        else
        {
            HashMap next;
            if (res.get(splitpipe[0]) == null)
            {
                next = new HashMap();
                res.put(splitpipe[0], next);
            }
            else {
                next = (HashMap)res.get(splitpipe[0]);
            }
           
            int index = theline.indexOf('|');
            DeserializeOneLine(theline.substring(index + 1), next);
        }
    }
       
    public static HashMap Deserialize(String mes)
    {
        HashMap res = new HashMap();
        String[] splitted = mes.replace("\r", "").split("\n");
        for (String split1 : splitted) {
            if (split1.startsWith("Main")) {
                try {
                    DeserializeOneLine(split1.substring(5), res);
                } catch (Exception ex) {
                    
                }
            }
        }
        return res;
    }    
}
