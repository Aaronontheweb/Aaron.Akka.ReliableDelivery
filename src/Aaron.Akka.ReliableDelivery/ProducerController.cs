using System;
using Akka.Actor;
using SeqNo = System.Int64;

namespace Aaron.Akka.ReliableDelivery
{
    public static class ProducerController
    {
        public sealed class Settings
        {
            public Settings(int? chunkLargeMessageBytes = null)
            {
                ChunkLargeMessageBytes = chunkLargeMessageBytes;
            }

            /// <summary>
            /// If non-null, determines the chunk size we use for transmitting large messages
            /// over the network to the consumer. If null, we don't chunk or serialize large messages.
            /// </summary>
            public int? ChunkLargeMessageBytes { get; }

            public Settings WithChunkLargeMessageBytes(int? chunkLargeMessageBytes)
            {
                return new Settings(chunkLargeMessageBytes);
            }
        }

        /// <summary>
        /// Marker interface to indicate that a message belongs to the reliable delivery domain.
        /// </summary>
        public interface IProducerCommand
        {
        }

        /// <summary>
        /// Marker interface for an output event from the producer controller.
        /// </summary>
        public interface IProducerEvent
        {
            string ProducerId { get; }
        }
        
        
        /// <summary>
        /// Message is sent to a producer to request the next message in a sequence
        /// </summary>
        public sealed class RequestNext<TMessage> : IProducerCommand
        {
            public RequestNext(string producerId, long currentSeqNo, long confirmedSeqNo, IActorRef sendConfirmationTo)
            {
                ProducerId = producerId;
                CurrentSeqNo = currentSeqNo;
                ConfirmedSeqNo = confirmedSeqNo;
                SendConfirmationTo = sendConfirmationTo;
            }

            public string ProducerId { get; }

            public SeqNo CurrentSeqNo { get; }

            public SeqNo ConfirmedSeqNo { get; }

            public IActorRef SendConfirmationTo { get; }
        }

        public sealed class MessageWithConfirmation<TMessage> : IProducerCommand, IDeliverySerializable
        {
            public MessageWithConfirmation(TMessage message, IActorRef sendConfirmationTo)
            {
                Message = message;
                ReplyTo = sendConfirmationTo;
            }

            public TMessage Message { get; }

            public IActorRef ReplyTo { get; }
        }
    }
}