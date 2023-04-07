using System;
using Akka.Actor;

namespace Aaron.Akka.ReliableDelivery
{
    public static class DeliveryProtocol
    {
        /// <summary>
        /// Instructions intended for producers.
        /// </summary>
        public interface IProducerCommand{ }
        
        /// <summary>
        /// Message sent from the Producer to the ProducerController to start the flow.
        /// </summary>
        /// <remarks>
        /// If a Producer actor ever restarts, it must send this message to the ProducerController.
        /// </remarks>
        /// <typeparam name="T">The type of messages supported by the ProducerController.</typeparam>
        public sealed class Start<T> : IProducerCommand
        {
            public Start(IActorRef producer)
            {
                Producer = producer;
            }
            
            public IActorRef Producer { get; }
        }

        /// <summary>
        /// Sent from the ProducerController to the Producer to request the next message in the sequence.
        /// </summary>
        /// <typeparam name="T">The type of messages supported by the ProducerController.</typeparam>
        public sealed class RequestNext<T> : IProducerCommand
        {
            public RequestNext(string producerId, long currentSeqNo, long confirmedSeqNo)
            {
                ProducerId = producerId;
                CurrentSeqNo = currentSeqNo;
                ConfirmedSeqNo = confirmedSeqNo;
            }

            public string ProducerId { get; }
            
            public long CurrentSeqNo { get; }
            
            public long ConfirmedSeqNo { get; }
        }

        /// <summary>
        /// Instructions intended for consumers
        /// </summary>
        public interface IConsumerCommand
        {
            
        }

        /// <summary>
        /// Message is sent to the <see cref="ProducerController{T}"/> actor to request the next messages
        /// in the sequence.
        /// </summary>
        /// <remarks>
        /// This message must be serializable.
        /// </remarks>
        public sealed class Request : IProducerCommand, IDeliverySerializable
        {
            public Request(long confirmedSeqNo, long requestUpToSeqNo, bool supportResend)
            {
                ConfirmedSeqNo = confirmedSeqNo;
                RequestUpToSeqNo = requestUpToSeqNo;
                
                // validate that ConfirmedSeqNo is less than or equal to RequestUpToSeqNo
                if (ConfirmedSeqNo > RequestUpToSeqNo)
                    throw new ArgumentOutOfRangeException(
                        $"ConfirmedSeqNo [{ConfirmedSeqNo}] should be <= RequestUpToSeqNo [{RequestUpToSeqNo}]");
                
                SupportResend = supportResend;
            }

            /// <summary>
            /// The most recently confirmed sequence number by the consumer.
            /// </summary>
            public long ConfirmedSeqNo { get; }
            
            /// <summary>
            /// Typically the next sequence number that is expected by the consumer.
            /// </summary>
            public long RequestUpToSeqNo { get; }
            
            /// <summary>
            /// Resend is only needed for point-to-point delivery, not for pull-based delivery.
            /// </summary>
            public bool SupportResend { get; }
        }
        
        /// <summary>
        /// Sent from a consumer controller to the actor.
        /// </summary>
        /// <typeparam name="T">The type of message being processed.</typeparam>
        public sealed class Delivery<T> : IConsumerCommand
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
            
            public Confirmed ToConfirmed() => new Confirmed(SeqNo, ProducerId);
        }
        
        public sealed class Confirmed : IConsumerCommand
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
}