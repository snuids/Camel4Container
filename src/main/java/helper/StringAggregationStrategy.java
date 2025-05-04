/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package helper;

import org.apache.camel.Exchange;
import org.apache.camel.AggregationStrategy;
//import org.apache.camel.processor.aggregate.TimeoutAwareAggregationStrategy;

/**
 *
 * @author Arnaud Marchand
 */
//simply combines Exchange String body values using '+' as a delimiter
class StringAggregationStrategy implements AggregationStrategy//,TimeoutAwareAggregationStrategy
{

    public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
        if (oldExchange == null) {
            return newExchange;
        }

        String oldBody = oldExchange.getIn().getBody(String.class);
        String newBody = newExchange.getIn().getBody(String.class);
        oldExchange.getIn().setBody(oldBody + newBody);
        return oldExchange;
    }

    @Override
    public void timeout(Exchange exchng, int i, int i1, long l)
    {
    }
}