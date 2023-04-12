using System;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Channels;
using Aaron.Akka.ReliableDelivery.Internal;
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
        public const int DefaultDeliveryBufferSize = 128;

        public Settings(bool requireConfirmationsToProducer, int deliveryBufferSize = DefaultDeliveryBufferSize,
            int? chunkLargeMessagesBytes = null)
        {
            ChunkLargeMessagesBytes = chunkLargeMessagesBytes;
            RequireConfirmationsToProducer = requireConfirmationsToProducer;
            DeliveryBufferSize = deliveryBufferSize;
        }

        /// <summary>
        /// If set to <c>null</c>, we will not chunk large messages. Otherwise, we will chunk messages larger than this value into [1,N] chunks of this size.
        /// </summary>
        public int? ChunkLargeMessagesBytes { get; }

        /// <summary>
        /// When set to <c>true</c>, ensures that confirmation messages are sent explicitly to the producer.
        /// </summary>
        public bool RequireConfirmationsToProducer { get; }

        /// <summary>
        /// How many unconfirmed messages can be pending in the buffer before we start backpressuring?
        /// </summary>
        public int DeliveryBufferSize { get; }
    }


    /// <summary>
    /// Commands that are specific to the producer side of the <see cref="ReliableDelivery"/> pattern.
    /// </summary>
    /// <typeparam name="T">The type of messages the producer manages.</typeparam>
    public interface IProducerCommand<T>
    {
    }

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
    public sealed class StartProduction<T> : IProducerCommand<T>, INoSerializationVerificationNeeded
    {
        public StartProduction(string producerId, ChannelWriter<T> writer)
        {
            ProducerId = producerId;
            Writer = writer;
        }

        public string ProducerId { get; }

        public ChannelWriter<T> Writer { get; }
    }

    /// <summary>
    /// Registers a ConsumerController with a ProducerController.
    /// </summary>
    public sealed class RegisterConsumer<T> : IProducerCommand<T>, IDeliverySerializable
    {
        public RegisterConsumer(IActorRef consumerController)
        {
            ConsumerController = consumerController;
        }

        public IActorRef ConsumerController { get; }
    }

    internal static void AssertLocalProducer(IActorRef producer)
    {
        if (producer is IActorRefScope { IsLocal: false })
            throw new ArgumentException(
                $"Producer [{producer}] must be local");
    }

    /// <summary>
    /// Commands hidden from the public interface.
    /// </summary>
    internal interface IInternalCommand
    {
    }

    /// <summary>
    /// Sent by the ConsumerController to the ProducerController to request the next messages in the buffer.
    /// </summary>
    internal readonly struct Request : IInternalCommand, IDeadLetterSuppression, IDeliverySerializable
    {
        public Request(long confirmedSeqNo, long requestUpToSeqNo, bool supportResend)
        {
            ConfirmedSeqNo = confirmedSeqNo;
            RequestUpToSeqNo = requestUpToSeqNo;
            SupportResend = supportResend;

            // assert that ConfirmedSeqNo <= RequestUpToSeqNo by throwing an ArgumentOutOfRangeException
            if (ConfirmedSeqNo > RequestUpToSeqNo)
                throw new ArgumentOutOfRangeException(nameof(confirmedSeqNo), confirmedSeqNo,
                    $"ConfirmedSeqNo [{confirmedSeqNo}] must be less than or equal to RequestUpToSeqNo [{requestUpToSeqNo}]");
        }

        /// <summary>
        /// Sequence numbers confirmed by the ConsumerController.
        /// </summary>
        public long ConfirmedSeqNo { get; }

        /// <summary>
        /// The next requested max sequence number.
        /// </summary>
        public long RequestUpToSeqNo { get; }

        /// <summary>
        /// Set to <c>false </c> in pull-mode.
        /// </summary>
        public bool SupportResend { get; }
    }
}

/// <summary>
/// INTERNAL API
/// </summary>
/// <typeparam name="T">The type of message handled by this producer</typeparam>
internal sealed class ProducerController<T> : ReceiveActor, IWithTimers
{
    public string ProducerId { get; }

    public State CurrentState { get; private set; }

    public ProducerController.Settings Settings { get; }

    private readonly Func<ConsumerController.SequencedMessage<T>, object> _sendAdapter;
    
    private readonly ILoggingAdapter _log = Context.GetLogger();
    private readonly Channel<T> _channel;
    private IActorRef _consumerController = ActorRefs.NoSender;
    private readonly CancellationTokenSource _shutdownCancellation = new();

    private CancellationToken ShutdownToken => _shutdownCancellation.Token;

    /// <summary>
    /// Default send function for when none are specified.
    /// </summary>
    private static readonly Func<ConsumerController.SequencedMessage<T>, object> DefaultSend = message => message;

    public ProducerController(string producerId, ProducerController.Settings settings,
        Func<ConsumerController.SequencedMessage<T>, object>? sendAdapter = null)
    {
        ProducerId = producerId;
        Settings = settings;
        _sendAdapter = sendAdapter ?? DefaultSend;
        _channel = Channel.CreateBounded<T>(new BoundedChannelOptions(Settings.DeliveryBufferSize)
        {
            SingleWriter = true, SingleReader = true, FullMode = BoundedChannelFullMode.Wait
        }); // force busy producers to wait
        CurrentState = new State(false, 0, 0, 0, 0, ImmutableList<ConsumerController.SequencedMessage<T>>.Empty,
            ActorRefs.NoSender);

        WaitingForActivation();
    }

    #region Internal Message and State Types

    /// <summary>
    /// The delivery state of the producer.
    /// </summary>
    public readonly struct State
    {
        public State(bool requested, long currentSeqNr, long confirmedSeqNr, long requestedSeqNr, long firstSeqNr,
            ImmutableList<ConsumerController.SequencedMessage<T>> unconfirmed, IActorRef? producer)
        {
            Requested = requested;
            CurrentSeqNr = currentSeqNr;
            ConfirmedSeqNr = confirmedSeqNr;
            RequestedSeqNr = requestedSeqNr;
            FirstSeqNr = firstSeqNr;
            Unconfirmed = unconfirmed;
            Producer = producer;
        }

        /// <summary>
        /// Has the consumer sent us their first request yet?
        /// </summary>
        public bool Requested { get; }

        /// <summary>
        /// Highest produced sequence number. Should always be less than or equal to <see cref="ConfirmedSeqNr"/>.
        /// </summary>
        public long CurrentSeqNr { get; }

        /// <summary>
        /// Highest confirmed sequence number
        /// </summary>
        public long ConfirmedSeqNr { get; }

        /// <summary>
        /// The current sequence number being requested by the consumer.
        /// </summary>
        public long RequestedSeqNr { get; }

        /// <summary>
        /// The first sequence number observed by this producer.
        /// </summary>
        public long FirstSeqNr { get; }

        /// <summary>
        /// The unconfirmed messages that have been sent to the consumer.
        /// </summary>
        public ImmutableList<ConsumerController.SequencedMessage<T>> Unconfirmed { get; }

        /// <summary>
        /// A reference to the producer actor.
        /// </summary>
        public IActorRef? Producer { get; }

        // copy state with new producer
        public State WithProducer(IActorRef producer) => new(Requested, CurrentSeqNr, ConfirmedSeqNr, RequestedSeqNr,
            FirstSeqNr, Unconfirmed, producer);

        // copy state with new requested sequence number
        public State WithRequestedSeqNr(long requestedSeqNr) => new(Requested, CurrentSeqNr, ConfirmedSeqNr,
            requestedSeqNr, FirstSeqNr, Unconfirmed, Producer);

        // copy state with new confirmed sequence number
        public State WithConfirmedSeqNr(long confirmedSeqNr) => new(Requested, CurrentSeqNr, confirmedSeqNr,
            RequestedSeqNr, FirstSeqNr, Unconfirmed, Producer);

        // copy state with new current sequence number
        public State WithCurrentSeqNr(long currentSeqNr) => new(Requested, currentSeqNr, ConfirmedSeqNr, RequestedSeqNr,
            FirstSeqNr, Unconfirmed, Producer);

        // copy state with new unconfirmed messages
        public State WithUnconfirmed(ImmutableList<ConsumerController.SequencedMessage<T>> unconfirmed) =>
            new(Requested, CurrentSeqNr, ConfirmedSeqNr, RequestedSeqNr, FirstSeqNr, unconfirmed, Producer);

        // copy state with new requested flag
        public State WithRequested(bool requested) => new(requested, CurrentSeqNr, ConfirmedSeqNr, RequestedSeqNr,
            FirstSeqNr, Unconfirmed, Producer);
    }

    /// <summary>
    /// Send the first message with the lowest delivery id.
    /// </summary>
    public sealed class ResendFirst
    {
        private ResendFirst()
        {
        }

        public static readonly ResendFirst Instance = new();
    }

    #endregion

    #region Behaviors

    private void WaitingForActivation()
    {
        // TODO: need to have durable state loading here also

        Receive<ProducerController.Start<T>>(start =>
        {
            ProducerController.AssertLocalProducer(start.Producer);
            CurrentState = CurrentState.WithProducer(start.Producer);

            // send ChannelWriter<T> to producer
            CurrentState.Producer.Tell(new ProducerController.StartProduction<T>(ProducerId, _channel.Writer));

            if (IsReadyForActivation)
            {
                BecomeActive();
            }
        });

        Receive<ProducerController.RegisterConsumer<T>>(consumer =>
        {
            _consumerController = consumer.ConsumerController;

            if (IsReadyForActivation)
            {
                BecomeActive();
            }
        });
    }

    private void BecomeActive()
    {
        var requested = false;
        if (CurrentState.Unconfirmed.IsEmpty)
        {
            requested = true;

            // kick off read task
            _channel.Reader.ReadAsync(ShutdownToken).PipeTo(Self);
        }
        else // will only be true if we've loaded our state from persistence
        {
            _log.Debug("Starting with [{0}] unconfirmed", CurrentState.Unconfirmed.Count);
            Self.Tell(ResendFirst.Instance);
            requested = false;
        }

        CurrentState = CurrentState.WithRequested(requested: requested);

        Become(Active);
    }

    private void Active()
    {
        Receive<T>(msg => { });
    }

    protected override void PostStop()
    {
        // terminate any in-flight requests
        _shutdownCancellation.Cancel();
        base.PostStop();
    }

    #endregion

    #region Internal Methods and Properties

    private bool IsReadyForActivation =>
        CurrentState.Producer != ActorRefs.NoSender && _consumerController != ActorRefs.NoSender;

    #endregion

    public ITimerScheduler Timers { get; set; }
}