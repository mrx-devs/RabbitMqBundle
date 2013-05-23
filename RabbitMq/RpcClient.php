<?php

namespace OldSound\RabbitMqBundle\RabbitMq;

use OldSound\RabbitMqBundle\RabbitMq\BaseAmqp;
use PhpAmqpLib\Message\AMQPMessage;

class RpcClient extends BaseAmqp
{
    protected $requests = 0;
    protected $replies = array();
    protected $queueName;
    protected $expiry_time = 0;
    protected $timeout = 0;
    
    public function initClient( $timeout = 10 )
    {
        $this->timeout = $timeout;
        list($this->queueName, ,) = $this->ch->queue_declare("", false, false, true, true);
    }

    public function addRequest($msgBody, $server, $requestId = null, $routingKey = '', $msgProperties = array() )
    {
        $default_message_properties = array(
            'content_type' => 'text/plain',
            'reply_to' => $this->queueName,
            'correlation_id' => $requestId,
        );
        
        $msgProperties = array_merge( $default_message_properties, $msgProperties );
        
        //Know how long we should wait for all messages to respond (if expiration times are set on ALL messages).
        if( array_key_exists('expiration',$msgProperties) )
        {
            if( $msgProperties['expiration'] > $this->expiry_time)
            {
                $this->expiry_time = (int) $msgProperties['expiration'];
            }
        }
        else
        {
            //If there's any message in this stack with no expiry time, then ignore this shortend expiry
            $this->expiry_time = 0;
        }
        
        
        if (empty($requestId)) {
            throw new \InvalidArgumentException('You must provide a $requestId');
        }

        $msg = new AMQPMessage($msgBody, $msgProperties);

        $this->ch->basic_publish($msg, $server, $routingKey);

        $this->requests++;
    }

    public function getReplies()
    {
        $this->ch->basic_consume($this->queueName, '', false, true, false, false, array($this, 'processMessage'));
        
        if($this->expiry_time)
        {
            //add a second just to be safe that the there's not any lengthy operations between the adding of the message
            //and the retrieval of responses.
            $timeout = (int) ($this->expiry_time / 1000) + 1; 
        }
        else
        {
            $timeout = $this->timeout;
        }
        
        while (count($this->replies) < $this->requests) {
            $this->ch->wait(null, false, $timeout);
        }
        
        $this->requests = 0; //reset this!
        $replies = $this->replies;
        $this->replies = array();
        $this->ch->basic_cancel($this->queueName);

        return $replies;
    }

    public function processMessage(AMQPMessage $msg)
    {
        //Used to be: $this->replies[$msg->get('correlation_id')] = unserialize($msg->body);
        $this->replies[$msg->get('correlation_id')] = $msg->body;
    }
    
    public function getTimeout()
    {
        return $this->timeout;
    }
}
