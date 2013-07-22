<?php

namespace OldSound\RabbitMqBundle\RabbitMq;

use OldSound\RabbitMqBundle\RabbitMq\BaseAmqp;
use PhpAmqpLib\Message\AMQPMessage;

class RpcClient extends BaseAmqp
{
    protected $requests = 0;
    protected $replies = array();
    protected $queueName;
    protected $timeout = 0;
    protected $messages = array();
    protected $queue_declared = false;
    
    public function initClient( $timeout = 10 )
    {
        $this->timeout = $timeout;
        $this->messages = array();
        if (!$this->queue_declared) {
            list($this->queueName, ,) = $this->ch->queue_declare("", false, false, true, true);
            $this->queue_declared = true;
        }
    }

    public function addRequest($msgBody, $server, $requestId = null, $routingKey = '', $msgProperties = array() )
    {
        $default_message_properties = array(
            'content_type' => 'text/plain',
            'reply_to' => $this->queueName,
            'correlation_id' => $requestId,
        );
        
        $msgProperties = array_merge( $default_message_properties, $msgProperties );
        
        if (empty($requestId)) {
            throw new \InvalidArgumentException('You must provide a $requestId');
        }

        $msg = new AMQPMessage($msgBody, $msgProperties);
        $this->messages[] = $msg;

        $this->ch->basic_publish($msg, $server, $routingKey);

        $this->requests++;
    }

    public function getReplies()
    {
        $max_wait = 0;
        $all_messages_expire = true;
        
        //Calculate the max amount of time we should wait if all messages have expiration times on them.
        foreach($this->messages as $message)
        {
            $props = $message->get_properties();
            
            if( array_key_exists('expiration', $props) && $props['expiration'] > $max_wait ){
                $max_wait = ceil( $props['expiration'] / 1000 );
            }
            else{
                $all_messages_expire = false;
                break; //we know max wait won't be used so get out of here
            }
        }
        
        //Don't wait longer than we have to. IE if it's only a read, and the message expires in 5 seconds why wait for full timeout
        $timeout =  ( $all_messages_expire ) ?  $max_wait : $this->timeout;
        
        $this->ch->basic_consume($this->queueName, '', false, true, false, false, array($this, 'processMessage'));
        
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
