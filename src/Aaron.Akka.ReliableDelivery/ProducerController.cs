using System;
using Akka.Actor;
using Akka.Event;
using Akka.Streams;

namespace Aaron.Akka.ReliableDelivery;

public static class ProducerController
{
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