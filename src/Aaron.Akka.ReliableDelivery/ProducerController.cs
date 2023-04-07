using System;
using System.Collections.Immutable;
using Aaron.Akka.ReliableDelivery.Internal;
using Akka.Actor;
using Akka.Event;
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
    /// Interface for all commands sent to or from the ProducerController.
    /// </summary>
    /// <typeparam name="T">The type of messages handled by the ProducerController.</typeparam>
    public interface IProducerCommand<T>
    {
    }

    /// <summary>
    /// Message sent from the Producer to the ProducerController to start the flow.
    /// </summary>
    /// <remarks>
    /// If a Producer actor ever restarts, it must send this message to the ProducerController.
    /// </remarks>
    /// <typeparam name="T">The type of messages supported by the ProducerController.</typeparam>
    public sealed class Start<T> : IProducerCommand<T>
    {
        public Start(IActorRef producer)
        {
            Producer = producer;
        }

        public IActorRef Producer { get; }
    }

    /// <summary>
    /// Registers a ConsumerController with a ProducerController.
    /// </summary>
    /// <typeparam name="T">The type of messages supported by the ProducerController.</typeparam>
    public sealed class RegisterConsumer<T> : IProducerCommand<T>, IDeliverySerializable
    {
        public RegisterConsumer(IActorRef consumer)
        {
            Consumer = consumer;
        }

        public IActorRef Consumer { get; }
    }

    /// <summary>
    /// Sent from the ProducerController to the Producer to request the next message in the sequence.
    /// </summary>
    /// <typeparam name="T">The type of messages supported by the ProducerController.</typeparam>
    public sealed class RequestNext<T> : IProducerCommand<T>
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
}

/// <summary>
/// INTERNAL API
/// </summary>
/// <remarks>
/// This is the point-to-point version of the reliable delivery flow.
/// </remarks>
/// <typeparam name="T">The type of messages supported by this <see cref="ProducerController"/></typeparam>
internal sealed class ProducerController<T> : UntypedActorWithUnboundedStash
{
    /// <summary>
    /// Default send function for when none are specified.
    /// </summary>
    internal static readonly Func<ConsumerController.SequencedMessage<T>, object> DefaultSend = message => message;

    public string ProducerId { get; }

    public State CurrentState { get; private set; }

    private IActorRef? _producer = ActorRefs.NoSender;
    private IActorRef? _consumerController = ActorRefs.NoSender;

    public ProducerController(string producerId, Func<ConsumerController.SequencedMessage<T>, object> send)
    {
        ProducerId = producerId;
        CurrentState = new State(false, 0L, 0L, 0L, 0L, ImmutableList<ConsumerController.SequencedMessage<T>>.Empty,
            null, send);
    }

    #region State Definition

    /// <summary>
    /// Internal state of the <see cref="ProducerController{T}"/> actor.
    /// </summary>
    public readonly struct State
    {
        public State(bool requested, long currentSeqNo, long confirmedSeqNo, long requestedSeqNo, long firstSeqNo,
            ImmutableList<ConsumerController.SequencedMessage<T>> unconfirmed,
            IActorRef? producer, Func<ConsumerController.SequencedMessage<T>, object> send)
        {
            Requested = requested;
            CurrentSeqNo = currentSeqNo;
            ConfirmedSeqNo = confirmedSeqNo;
            RequestedSeqNo = requestedSeqNo;
            Producer = producer;
            Send = send;
            Unconfirmed = unconfirmed;
            FirstSeqNo = firstSeqNo;
        }

        /// <summary>
        /// Indicates whether or not the consumer has requested the first message in the sequence.
        /// </summary>
        public bool Requested { get; }

        /// <summary>
        /// The current max sequence number set by the producer's production.
        /// </summary>
        public long CurrentSeqNo { get; }

        /// <summary>
        /// The current max confirmed sequence number set by the consumer.
        /// </summary>
        public long ConfirmedSeqNo { get; }

        /// <summary>
        /// The highest requested sequence number by the consumer.
        /// </summary>
        /// <remarks>
        /// Must be greater than or equal to <see cref="ConfirmedSeqNo"/>.
        /// </remarks>
        public long RequestedSeqNo { get; }

        /// <summary>
        /// The first sequence number observed by this ProducerController.
        /// </summary>
        public long FirstSeqNo { get; }

        public ImmutableList<ConsumerController.SequencedMessage<T>> Unconfirmed { get; }

        /// <summary>
        /// The producer actor. Only populated once a <see cref="ProducerController.Start{T}"/> message is received.
        /// </summary>
        public IActorRef? Producer { get; }

        /// <summary>
        /// The adapter function used to optionally wrap messages before sending them to the consumer.
        /// </summary>
        public Func<ConsumerController.SequencedMessage<T>, object> Send { get; }

        // create a method that allows for immutable copying of all properties, but also overriding of specific properties
        public State With(bool? requested = null, long? currentSeqNo = null, long? confirmedSeqNo = null,
            long? requestedSeqNo = null, long? firstSeqNo = null,
            ImmutableList<ConsumerController.SequencedMessage<T>>? unconfirmed = null, IActorRef? producer = null)
        {
            return new State(
                requested ?? Requested,
                currentSeqNo ?? CurrentSeqNo,
                confirmedSeqNo ?? ConfirmedSeqNo,
                requestedSeqNo ?? RequestedSeqNo,
                firstSeqNo ?? FirstSeqNo,
                unconfirmed ?? Unconfirmed,
                producer ?? Producer, Send);
        }
    }

    #endregion

    #region Internal Messages

    /// <summary>
    /// INTERNAL API
    /// </summary>
    public interface IInternalProducerCommand
    {
    }

    /// <summary>
    /// Message is sent to the <see cref="ProducerController{T}"/> actor to request the next messages
    /// in the sequence.
    /// </summary>
    /// <remarks>
    /// This message must be serializable.
    /// </remarks>
    public sealed class Request : IInternalProducerCommand, IDeliverySerializable, IDeadLetterSuppression
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

    public sealed class Resend : IInternalProducerCommand, IDeliverySerializable, IDeadLetterSuppression
    {
        public Resend(long fromSeqNo)
        {
            FromSeqNo = fromSeqNo;
        }

        public long FromSeqNo { get; }
    }

    public sealed class Ack : IInternalProducerCommand, IDeliverySerializable, IDeadLetterSuppression
    {
        public Ack(long confirmedSeqNo)
        {
            ConfirmedSeqNo = confirmedSeqNo;
        }

        public long ConfirmedSeqNo { get; }
    }

    #endregion


   
    protected override void OnReceive(object message)
    {
    }
}