using System;
using System.Threading.Channels;
using Akka.Actor;
using Akka.Event;
using Akka.Serialization;
using Akka.Streams;

namespace Aaron.Akka.ReliableDelivery;

public static class ProducerController
{
    // TODO: HOCON configuration
    public sealed class Settings
    {
        public Settings(int? chunkLargeMessagesBytes)
        {
            ChunkLargeMessagesBytes = chunkLargeMessagesBytes;
        }

        /// <summary>
        /// If set to <c>null</c>, we will not chunk large messages. Otherwise, we will chunk messages larger than this value into [1,N] chunks of this size.
        /// </summary>
        public int? ChunkLargeMessagesBytes { get; }
    }

    
    /// <summary>
    /// Commands that are specific to the producer side of the <see cref="ReliableDelivery"/> pattern.
    /// </summary>
    /// <typeparam name="T">The type of messages the producer manages.</typeparam>
    public interface IProducerCommand<T>{ }
    
    /// <summary>
    /// Signal to the ProducerController that we're ready to begin message production.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public sealed class Start<T> : IProducerCommand<T>
    {
        public Start(IActorRef producer)
        {
            Producer = producer;
        }
        
        public IActorRef Producer { get; }
    }
    
    /// <summary>
    /// Message send back to the producer in response to a <see cref="Start{T}"/> command.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public sealed class ProducerStarted<T> : IProducerCommand<T>, INoSerializationVerificationNeeded
    {
        public ProducerStarted(string producerId, ChannelWriter<T> writer)
        {
            ProducerId = producerId;
            Writer = writer;
        }
        
        public string ProducerId { get; }

        public ChannelWriter<T> Writer { get; }
    }
}

/// <summary>
/// INTERNAL API
/// </summary>
/// <typeparam name="T">The type of message handled by this producer</typeparam>
internal sealed class ProducerController<T> : ReceiveActor
{
    public string ProducerId { get; }
    private readonly Lazy<IMaterializer> _materializer = new(() => Context.Materializer());
    private readonly ILoggingAdapter _log = Context.GetLogger();
    
}