using Aaron.Akka.ReliableDelivery.Internal;
using Akka.Actor;
using Akka.Annotations;

namespace Aaron.Akka.ReliableDelivery;

public class DurableProducerQueue
{
    /// <summary>
    /// Commands send to the durable producer queue.
    /// </summary>
    /// <typeparam name="T">The type of messages handled by this durable queue.</typeparam>
    public interface IQueueCommand<T>
    {
    }

    /// <summary>
    /// An instruction used to load the current state snapshot to the caller.
    /// </summary>
    public sealed class LoadState<T> : IQueueCommand<T>
    {
        public LoadState(IActorRef replyTo)
        {
            ReplyTo = replyTo;
        }

        public IActorRef ReplyTo { get; }
    }
    
    public sealed class StoreMessageSent<T> : IQueueCommand<T>
    {
        public StoreMessageSent(MessageSent<T> messageSent, IActorRef replyTo)
        {
            MessageSent = messageSent;
            ReplyTo = replyTo;
        }

        public MessageSent<T> MessageSent { get; }
        
        public IActorRef ReplyTo { get; }
    }

    public sealed class StoreMessageSentAck
    {
        public StoreMessageSentAck(long storedSeqNo)
        {
            StoredSeqNo = storedSeqNo;
        }

        public long StoredSeqNo { get; }
    }
    
    /// <summary>
    /// Store a record that the message with the given sequence number has been confirmed.
    /// </summary>
    public sealed class StoreMessageConfirmed<T> : IQueueCommand<T>
    {
        public StoreMessageConfirmed(long seqNo, long timeStamp)
        {
            SeqNo = seqNo;
            TimeStamp = timeStamp;
        }

        public long SeqNo { get; }
        
        public long TimeStamp { get; }
    }

    /// <summary>
    /// Used to denote queue events for event-sourcing purposes
    /// </summary>
    [InternalApi]
    public interface IQueueEvent : IDeliverySerializable
    {
        
    }

    /// <summary>
    /// An event indicating that a message has been sent.
    /// </summary>
    public sealed class MessageSent<T> : IQueueEvent
    {
        public MessageSent(T message, long seqNo, string producerId, long timeStamp)
        {
            Message = message;
            SeqNo = seqNo;
            ProducerId = producerId;
            TimeStamp = timeStamp;
        }

        public T Message { get; }
        public long SeqNo { get; }
        public string ProducerId { get; }

        /// <summary>
        /// Timestamp uses an epoch to denote specific send / transmission events, i.e. the current system time.
        /// </summary>
        public long TimeStamp { get; }

        public MessageSent<T> WithTimestamp(long timeStamp) => new(Message, SeqNo, ProducerId, timeStamp);
    }

    /// <summary>
    /// INTERNAL API
    ///
    /// An event indicating that a message has been delivered and processed by the receiver.
    /// </summary>
    [InternalApi]
    public sealed class Confirmed : IQueueEvent
    {
        public Confirmed(long seqNo, long timeStamp)
        {
            SeqNo = seqNo;
            TimeStamp = timeStamp;
        }

        public long SeqNo { get; }
        
        public long TimeStamp { get; }
    }

    public sealed class State<T> : IDeliverySerializable
    {
        public long CurrentSeqNo { get; }
        
        public long HighestConfirmedSeqNo { get; }
    }
}