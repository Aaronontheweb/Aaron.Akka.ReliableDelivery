// -----------------------------------------------------------------------
//  <copyright file="ProducerController.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2023 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Channels;
using Aaron.Akka.ReliableDelivery.Internal;
using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
using Akka.Util;

namespace Aaron.Akka.ReliableDelivery;

public static class ProducerController
{
    internal static void AssertLocalProducer(IActorRef producer)
    {
        if (producer is IActorRefScope { IsLocal: false })
            throw new ArgumentException(
                $"Producer [{producer}] must be local");
    }

    public static Props PropsFor<T>(string producerId, Option<Props> durableProducerQueue, Settings? settings = null,
        Func<ConsumerController.SequencedMessage<T>, object>? sendAdapter = null)
    {
        return Props.Create(() => new ProducerController<T>(producerId, durableProducerQueue, settings, DateTimeOffsetNowTimeProvider.Instance, sendAdapter));   
    }

    // TODO: HOCON configuration
    public sealed class Settings
    {
        public const int DefaultDeliveryBufferSize = 128;

        public static Settings Create(ActorSystem actorSystem)
        {
            return Create(actorSystem.Settings.Config.GetConfig("aaron.akka.reliable-delivery.producer-controller")!);
        }

        public static Settings Create(Config config)
        {
            var chunkLargeMessageBytes = config.GetString("chunk-large-messages") switch {
                "off" => 0,
                _ => (config.GetByteSize("chunk-large-messages") ?? throw new ArgumentException("chunk-large-messages must be set to a valid byte size")),
            };
            
            if(chunkLargeMessageBytes > int.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(config),"Too large chunk-large-messages value. Must be less than 2GB");

            return new Settings(false, durableQueueRequestTimeout: config.GetTimeSpan("durable-queue.request-timeout"),
                durableQueueRetryAttempts: config.GetInt("durable-queue.retry-attempts"),
                durableQueueResendFirstInterval: config.GetTimeSpan("durable-queue.resend-first-interval"),
                deliveryBufferSize: config.GetInt("delivery-buffer-size", 12),
                chunkLargeMessagesBytes: (int)chunkLargeMessageBytes);
        }

        public Settings(bool requireConfirmationsToProducer, TimeSpan durableQueueRequestTimeout,
            int durableQueueRetryAttempts, TimeSpan durableQueueResendFirstInterval, int deliveryBufferSize = DefaultDeliveryBufferSize,
            int? chunkLargeMessagesBytes = null)
        {
            ChunkLargeMessagesBytes = chunkLargeMessagesBytes;
            RequireConfirmationsToProducer = requireConfirmationsToProducer;
            DurableQueueRequestTimeout = durableQueueRequestTimeout;
            DurableQueueRetryAttempts = durableQueueRetryAttempts;
            DurableQueueResendFirstInterval = durableQueueResendFirstInterval;
            DeliveryBufferSize = deliveryBufferSize;
        }

        /// <summary>
        ///     If set to <c>null</c>, we will not chunk large messages. Otherwise, we will chunk messages larger than this value
        ///     into [1,N] chunks of this size.
        /// </summary>
        public int? ChunkLargeMessagesBytes { get; }

        /// <summary>
        ///     When set to <c>true</c>, ensures that confirmation messages are sent explicitly to the producer.
        /// </summary>
        public bool RequireConfirmationsToProducer { get; }

        /// <summary>
        ///     How many unconfirmed messages can be pending in the buffer before we start backpressuring?
        /// </summary>
        public int DeliveryBufferSize { get; }

        /// <summary>
        /// The timeout for each request to the durable queue.
        /// </summary>
        public TimeSpan DurableQueueRequestTimeout { get; }

        /// <summary>
        /// Number of retries allowed for each request to the durable queue.
        /// </summary>
        public int DurableQueueRetryAttempts { get; }

        /// <summary>
        /// Timeframe for re-delivery of the first message
        /// </summary>
        public TimeSpan DurableQueueResendFirstInterval { get; }
    }


    /// <summary>
    ///     Commands that are specific to the producer side of the <see cref="RdConfig" /> pattern.
    /// </summary>
    /// <typeparam name="T">The type of messages the producer manages.</typeparam>
    public interface IProducerCommand<T>
    {
    }

    /// <summary>
    ///     Signal to the ProducerController that we're ready to begin message production.
    /// </summary>
    public sealed class Start<T> : IProducerCommand<T>
    {
        public Start(IActorRef producer)
        {
            Producer = producer;
        }

        public IActorRef Producer { get; }
    }

    /// <summary>
    ///     Message send back to the producer in response to a <see cref="Start{T}" /> command.
    /// </summary>
    public sealed class StartProduction<T> : IProducerCommand<T>, INoSerializationVerificationNeeded
    {
        public StartProduction(string producerId, ChannelWriter<SendNext<T>> writer)
        {
            ProducerId = producerId;
            Writer = writer;
        }

        public string ProducerId { get; }

        public ChannelWriter<SendNext<T>> Writer { get; }
    }

    /// <summary>
    ///     A send instruction sent from Producers to Consumers.
    /// </summary>
    public sealed class SendNext<T> : IProducerCommand<T>, INoSerializationVerificationNeeded
    {
        public SendNext(T message, IActorRef? sendConfirmationTo)
        {
            Message = message;
            SendConfirmationTo = sendConfirmationTo;
        }

        /// <summary>
        ///     The message that will actually be delivered to consumers.
        /// </summary>
        public T Message { get; }

        /// <summary>
        ///     If this field is populated, confirmation messages containing the current SeqNo (long) will be sent to this actor.
        /// </summary>
        public IActorRef? SendConfirmationTo { get; }
    }

    /// <summary>
    ///     Registers a ConsumerController with a ProducerController.
    /// </summary>
    public sealed class RegisterConsumer<T> : IProducerCommand<T>, IDeliverySerializable
    {
        public RegisterConsumer(IActorRef consumerController)
        {
            ConsumerController = consumerController;
        }

        public IActorRef ConsumerController { get; }
    }

    /// <summary>
    ///     Commands hidden from the public interface.
    /// </summary>
    internal interface IInternalCommand
    {
    }

    internal sealed class Resend : IInternalCommand, IDeliverySerializable, IDeadLetterSuppression
    {
        public Resend(long fromSeqNr)
        {
            FromSeqNr = fromSeqNr;
        }

        public long FromSeqNr { get; }
    }
    
    internal sealed class Ack : IInternalCommand, IDeliverySerializable, IDeadLetterSuppression
    {
        public Ack(long confirmedSeqNr)
        {
            ConfirmedSeqNr = confirmedSeqNr;
        }

        public long ConfirmedSeqNr { get; }
    }
    
    
    /// <summary>
    ///     Send the first message with the lowest delivery id.
    /// </summary>
    internal sealed class ResendFirst : IInternalCommand
    {
        public static readonly ResendFirst Instance = new();

        private ResendFirst()
        {
        }
    }

    internal sealed class ResendFirstUnconfirmed: IInternalCommand
    {
        public static readonly ResendFirstUnconfirmed Instance = new();
        private ResendFirstUnconfirmed(){}
    }

    internal sealed class SendChunk : IInternalCommand
    {
        public static readonly SendChunk Instance = new();
        private SendChunk(){}
    }

    /// <summary>
    ///     Sent by the ConsumerController to the ProducerController to request the next messages in the buffer.
    /// </summary>
    internal sealed class Request : IInternalCommand, IDeadLetterSuppression, IDeliverySerializable
    {
        public Request(long confirmedSeqNo, long requestUpToSeqNo, bool supportResend, bool viaTimeout)
        {
            ConfirmedSeqNo = confirmedSeqNo;
            RequestUpToSeqNo = requestUpToSeqNo;
            SupportResend = supportResend;
            ViaTimeout = viaTimeout;

            // assert that ConfirmedSeqNo <= RequestUpToSeqNo by throwing an ArgumentOutOfRangeException
            if (ConfirmedSeqNo > RequestUpToSeqNo)
                throw new ArgumentOutOfRangeException(nameof(confirmedSeqNo), confirmedSeqNo,
                    $"ConfirmedSeqNo [{confirmedSeqNo}] must be less than or equal to RequestUpToSeqNo [{requestUpToSeqNo}]");
        }

        /// <summary>
        ///     Sequence numbers confirmed by the ConsumerController.
        /// </summary>
        public long ConfirmedSeqNo { get; }

        /// <summary>
        ///     The next requested max sequence number.
        /// </summary>
        public long RequestUpToSeqNo { get; }

        /// <summary>
        ///     Set to <c>false </c> in pull-mode.
        /// </summary>
        public bool SupportResend { get; }
        
        public bool ViaTimeout { get; }
    }

    internal sealed class LoadStateReply<T> : IInternalCommand
    {
        public LoadStateReply(DurableProducerQueue.State<T> state)
        {
            State = state;
        }

        public DurableProducerQueue.State<T> State { get; }
    }

    internal sealed class LoadStateFailed : IInternalCommand
    {
        public LoadStateFailed(int attempts)
        {
            Attempts = attempts;
        }

        public int Attempts { get; }
    }

    internal sealed class StoreMessageSentReply : IInternalCommand
    {
        public StoreMessageSentReply(DurableProducerQueue.StoreMessageSentAck ack)
        {
            Ack = ack;
        }

        public DurableProducerQueue.StoreMessageSentAck Ack { get; }
    }

    internal sealed class StoreMessageSentFailed<T> : IInternalCommand
    {
        public StoreMessageSentFailed(DurableProducerQueue.MessageSent<T> messageSent, int attempt)
        {
            MessageSent = messageSent;
            Attempt = attempt;
        }

        public DurableProducerQueue.MessageSent<T> MessageSent { get; }

        public int Attempt { get; }
    }

    internal sealed class StoreMessageSentCompleted<T> : IInternalCommand
    {
        public StoreMessageSentCompleted(DurableProducerQueue.MessageSent<T> messageSent)
        {
            MessageSent = messageSent;
        }

        public DurableProducerQueue.MessageSent<T> MessageSent { get; }
    }

    internal sealed class DurableQueueTerminated : IInternalCommand
    {
        private DurableQueueTerminated()
        {
        }

        public static DurableQueueTerminated Instance { get; } = new();
    }
}