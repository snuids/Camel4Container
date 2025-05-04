/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pubsub.website;

/**
 *
 * @author Arnaud Marchand
 */
class OneMessage
{
    public String PubSubID;
    public String Message;
    public String ClientName;

    public OneMessage(String PubSubID,String ClientName,String Message)
    {
        this.PubSubID = PubSubID;
        this.Message = Message;
        this.ClientName=ClientName;
    }
    
}
