namespace Aaron.Akka.ReliableDelivery;

public static class ConsumerController
{
    /// <summary>
    /// Interface for all commands sent to or from the ConsumerController.
    /// </summary>
    /// <typeparam name="T">The type of messages handled by the ConsumerController.</typeparam>
    public interface IConsumerCommand<T>{}
    
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
}