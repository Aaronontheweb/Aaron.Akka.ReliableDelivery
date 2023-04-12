using Aaron.Akka.ReliableDelivery.Internal;
using Akka.Actor;
using Akka.Annotations;
using Akka.Event;
using Akka.Util;

namespace Aaron.Akka.ReliableDelivery;

public static class ConsumerController
{
    /// <summary>
    /// Commands that are specific to the consumer side of the <see cref="ReliableDelivery"/> pattern.
    /// </summary>
    /// <typeparam name="T">The type of messages the consumer manages.</typeparam>
    public interface IConsumerCommand<T>{ }
    
    /// <summary>
    /// Used to signal to the ConsumerController that we're ready to start message production.
    /// </summary>
    public sealed class Start<T> : IConsumerCommand<T>
    {
        public Start(IActorRef consumer)
        {
            Consumer = consumer;
        }
        
        public IActorRef Consumer { get; }
    }

    public readonly struct MessageOrChunk<T>
    {
        public MessageOrChunk(T message)
        {
            Message = message;
            Chunk = null;
        }
        
        public MessageOrChunk(ChunkedMessage chunkedMessage)
        {
            Message = default;
            Chunk = chunkedMessage;
        }

        public T? Message { get; }
        
        public ChunkedMessage? Chunk { get; }
        
        public bool IsMessage => Message != null;
        
        public static implicit operator MessageOrChunk<T>(T message) => new(message);
        
        public static implicit operator MessageOrChunk<T>(ChunkedMessage chunkedMessage) => new(chunkedMessage);
    }
    
    /// <summary>
    /// A sequenced message that is delivered to the consumer via the ProducerController.
    /// </summary>
    [InternalApi]
    public sealed class SequencedMessage<T> : IConsumerCommand<T>, IDeliverySerializable, IDeadLetterSuppression
    {
        public SequencedMessage(string producerId, long seqNr, MessageOrChunk<T> messageOrChunk, bool first, bool ack)
        {
            SeqNr = seqNr;
            Message = messageOrChunk;
            First = first;
            Ack = ack;
            ProducerId = producerId;
        }

        public long SeqNr { get; }
        
        public string ProducerId { get; }
        public MessageOrChunk<T> Message { get; }
        
        public bool First { get; }
        
        public bool Ack { get; }

        internal bool IsFirstChunk => Message.Chunk is { FirstChunk: true };
        
        internal bool IsLastChunk => Message.Chunk is { LastChunk: true };
        
        internal static SequencedMessage<T> FromChunkedMessage(string producerId, long seqNr, ChunkedMessage chunkedMessage, bool first, bool ack)
        {
            return new(producerId, seqNr, chunkedMessage, first, ack);
        }
    }
    
    /// <summary>
    /// Sent from the consumer controller to the consumer.
    /// </summary>
    public sealed class Delivery<T> : IConsumerCommand<T>, IDeliverySerializable, IDeadLetterSuppression
    {
        public Delivery(long seqNr, string producerId, T message)
        {
            SeqNr = seqNr;
            Message = message;
            ProducerId = producerId;
        }

        public long SeqNr { get; }
        
        public string ProducerId { get; }
        public T Message { get; }
        
        /// <summary>
        /// Creates a confirmation message that can be sent back to the producer.
        /// </summary>
        public Confirmed<T> Confirmation => new(ProducerId, SeqNr);
    }

    /// <summary>
    /// Acknowledgement of a message that was received by the consumer, sent to the ConsumerController.
    /// </summary>
    public sealed class Confirmed<T> : IConsumerCommand<T>
    {
        public Confirmed(string producerId, long confirmedSeqNr)
        {
            ProducerId = producerId;
            ConfirmedSeqNr = confirmedSeqNr;
        }

        public string ProducerId { get; }
        
        public long ConfirmedSeqNr { get; }
    }

    /// <summary>
    /// Send from the ConsumerController to the ProducerController to request more messages.
    /// </summary>
    public sealed class Request<T> : IDeliverySerializable, IDeadLetterSuppression
    {
        public Request(string producerId, long fromSeqNr, long confirmedSeqNr)
        {
            ProducerId = producerId;
            FromSeqNr = fromSeqNr;
            ConfirmedSeqNr = confirmedSeqNr;
        }

        public string ProducerId { get; }
        
        public long FromSeqNr { get; }
        
        public long ConfirmedSeqNr { get; }
    }
}