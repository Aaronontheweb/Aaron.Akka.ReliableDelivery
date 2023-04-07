using Aaron.Akka.ReliableDelivery.Internal;
using Akka.Actor;
using Akka.Event;

namespace Aaron.Akka.ReliableDelivery;

public static class ConsumerController
{
    /// <summary>
    /// Interface for all commands sent to or from the ConsumerController.
    /// </summary>
    /// <typeparam name="T">The type of messages handled by the ConsumerController.</typeparam>
    public interface IConsumerCommand<T>{}
    
    /// <summary>
    /// Signals that the consumer is ready to start consuming.
    /// </summary>
    public sealed class Start<T> : IConsumerCommand<T>
    {
        public Start(IActorRef consumer)
        {
            Consumer = consumer;
        }
        
        public IActorRef Consumer { get; }
    }
    
    /// <summary>
    /// Sent from a consumer controller to the actor.
    /// </summary>
    /// <typeparam name="T">The type of message being processed.</typeparam>
    public sealed class Delivery<T> : IConsumerCommand<T>
    {
        public Delivery(T message, long seqNo, string producerId)
        {
            Message = message;
            SeqNo = seqNo;
            ProducerId = producerId;
        }

        public T Message { get; }
        public long SeqNo { get; }
        public string ProducerId { get; }
            
        /// <summary>
        /// Generates a <see cref="Confirmed{T}"/> message to be sent back to the <see cref="ConsumerController{T}"/>.
        /// </summary>
        public Confirmed<T> ToConfirmed() => new(SeqNo, ProducerId);
    }
        
    /// <summary>
    /// Sent from the consumer actor back to the ConsumerController to confirm that the message was processed.
    /// </summary>
    public sealed class Confirmed<T> : IConsumerCommand<T>
    {
        public Confirmed(long seqNo, string producerId)
        {
            SeqNo = seqNo;
            ProducerId = producerId;
        }

        public long SeqNo { get; }
        public string ProducerId { get; }
    }
    
    /// <summary>
    /// This message is used to exchange messages between the <see cref="ProducerController"/> and the <see cref="ConsumerController"/>.
    /// </summary>
    /// <remarks>
    /// Not really meant to be called from within application code.
    /// </remarks>
    public sealed class SequencedMessage<T> : IConsumerCommand<T>, IDeliverySerializable, IDeadLetterSuppression
    {
        public SequencedMessage(string producerId, long seqNo, T message, bool ack)
        {
            ProducerId = producerId;
            SeqNo = seqNo;
            Message = message;
            Ack = ack;
        }

        public string ProducerId { get; }
        public long SeqNo { get; }
        public T Message { get; }
        
        /// <summary>
        /// When <c>true</c>, means we expect an <see cref="Ack"/> message back from the consumer.
        /// </summary>
        /// <remarks>
        /// This happens in Point-to-Point mode, but not in Consumer-Pull mode.
        /// </remarks>
        public bool Ack { get; }
    }
}