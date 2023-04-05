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
        public interface IProducerCommand : IDeliverySerializable
        {
        }

        /// <summary>
        /// Marker interface for an output event from the producer controller.
        /// </summary>
        public interface IProducerEvent : IDeliverySerializable
        {
            string ProducerId { get; }
        }
    }
    
   
    
    /// <summary>
    /// A producer controller - takes messages from a local actor and uses
    /// them to guarantee 
    /// </summary>
    /// <typeparam name="TMessage">Should ideally be a marker interface</typeparam>
    public class ProducerController<TMessage> : ReceiveActor
    {
        private readonly string _producerId;
        private IActorRef _producer;
        private IActorRef _consumerController;
        private readonly ProducerController.Settings _settings;
        public ProducerController(string producerId, ProducerController.Settings settings)
        {
            _settings = settings;
            _producerId = producerId;
        }
    }

    /// <summary>
    /// A single producer matches to a single ProducerController. This message
    /// is used to register the producer with the ProducerController and signal that message
    /// production is ready to begin.
    /// </summary>
    public sealed class Start : ProducerController.IProducerCommand
    {
        public Start(IActorRef producer)
        {
            Producer = producer;
        }

        public IActorRef Producer { get; }
    }

    public class RegisterConsumer : ProducerController.IProducerCommand
    {
        public RegisterConsumer(IActorRef consumer)
        {
            Consumer = consumer;
        }

        public IActorRef Consumer { get; }
    }
}
