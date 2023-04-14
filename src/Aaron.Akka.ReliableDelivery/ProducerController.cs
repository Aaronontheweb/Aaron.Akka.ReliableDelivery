﻿// -----------------------------------------------------------------------
//  <copyright file="ProducerController.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2023 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using Aaron.Akka.ReliableDelivery.Internal;
using Akka.Actor;
using Akka.Event;
using Akka.IO;
using Akka.Pattern;
using Akka.Serialization;
using Akka.Util;
using Microsoft.Extensions.ObjectPool;

namespace Aaron.Akka.ReliableDelivery;

public static class ProducerController
{
    internal static void AssertLocalProducer(IActorRef producer)
    {
        if (producer is IActorRefScope { IsLocal: false })
            throw new ArgumentException(
                $"Producer [{producer}] must be local");
    }

    // TODO: HOCON configuration
    public sealed class Settings
    {
        public const int DefaultDeliveryBufferSize = 128;

        public Settings(bool requireConfirmationsToProducer, TimeSpan durableQueueRequestTimeout, int durableQueueAttemptRetries, int deliveryBufferSize = DefaultDeliveryBufferSize,
            int? chunkLargeMessagesBytes = null)
        {
            ChunkLargeMessagesBytes = chunkLargeMessagesBytes;
            RequireConfirmationsToProducer = requireConfirmationsToProducer;
            DurableQueueRequestTimeout = durableQueueRequestTimeout;
            DurableQueueAttemptRetries = durableQueueAttemptRetries;
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
        public int DurableQueueAttemptRetries { get; }
    }


    /// <summary>
    ///     Commands that are specific to the producer side of the <see cref="ReliableDelivery" /> pattern.
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

    /// <summary>
    ///     Sent by the ConsumerController to the ProducerController to request the next messages in the buffer.
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

/// <summary>
///     INTERNAL API
/// </summary>
/// <typeparam name="T">The type of message handled by this producer</typeparam>
internal sealed class ProducerController<T> : ReceiveActor, IWithTimers
{
    /// <summary>
    ///     Default send function for when none are specified.
    /// </summary>
    private static readonly Func<ConsumerController.SequencedMessage<T>, object> DefaultSend = message => message;

    private readonly Channel<ProducerController.SendNext<T>> _channel;

    private readonly ILoggingAdapter _log = Context.GetLogger();

    private readonly Func<ConsumerController.SequencedMessage<T>, object> _sendAdapter;
    private readonly Lazy<Serialization> _serialization = new(() => Context.System.Serialization);
    private readonly CancellationTokenSource _shutdownCancellation = new();
    private IActorRef _consumerController = ActorRefs.NoSender;
    private Option<Props> _durableProducerQueueProps;

    public ProducerController(string producerId, ProducerController.Settings settings,
        Option<Props> durableProducerQueue,
        Func<ConsumerController.SequencedMessage<T>, object>? sendAdapter = null)
    {
        ProducerId = producerId;
        Settings = settings;
        _durableProducerQueueProps = durableProducerQueue;
        _sendAdapter = sendAdapter ?? DefaultSend;
        _channel = Channel.CreateBounded<ProducerController.SendNext<T>>(
            new BoundedChannelOptions(Settings.DeliveryBufferSize)
            {
                SingleWriter = true, SingleReader = true, FullMode = BoundedChannelFullMode.Wait
            }); // force busy producers to wait
        CurrentState = new State(false, 0, 0, 0, true, 0, ImmutableList<ConsumerController.SequencedMessage<T>>.Empty,
            ActorRefs.NoSender, ImmutableList<ConsumerController.SequencedMessage<T>>.Empty,
            ImmutableDictionary<long, IActorRef>.Empty);

        WaitingForActivation();
    }

    public string ProducerId { get; }

    public State CurrentState { get; private set; }

    public Option<IActorRef> DurableProducerQueueRef { get; private set; }

    public ProducerController.Settings Settings { get; }

    private CancellationToken ShutdownToken => _shutdownCancellation.Token;

    public ITimerScheduler Timers { get; set; } = null!;

    #region Internal Message and State Types

    /// <summary>
    ///     The delivery state of the producer.
    /// </summary>
    public readonly struct State
    {
        public State(bool requested, long currentSeqNr, long confirmedSeqNr, long requestedSeqNr, bool supportResend,
            long firstSeqNr,
            ImmutableList<ConsumerController.SequencedMessage<T>> unconfirmed, IActorRef? producer,
            ImmutableList<ConsumerController.SequencedMessage<T>> remainingChunks,
            ImmutableDictionary<long, IActorRef> replyAfterStore)
        {
            Requested = requested;
            CurrentSeqNr = currentSeqNr;
            ConfirmedSeqNr = confirmedSeqNr;
            RequestedSeqNr = requestedSeqNr;
            FirstSeqNr = firstSeqNr;
            Unconfirmed = unconfirmed;
            Producer = producer;
            RemainingChunks = remainingChunks;
            ReplyAfterStore = replyAfterStore;
            SupportResend = supportResend;
        }

        /// <summary>
        ///     Has the consumer sent us their first request yet?
        /// </summary>
        public bool Requested { get; }

        /// <summary>
        ///     Highest produced sequence number. Should always be less than or equal to <see cref="ConfirmedSeqNr" />.
        /// </summary>
        public long CurrentSeqNr { get; }

        /// <summary>
        ///     Highest confirmed sequence number
        /// </summary>
        public long ConfirmedSeqNr { get; }

        /// <summary>
        ///     The current sequence number being requested by the consumer.
        /// </summary>
        public long RequestedSeqNr { get; }

        public bool SupportResend { get; }

        public ImmutableDictionary<long, IActorRef> ReplyAfterStore { get; }

        /// <summary>
        ///     The first sequence number in this state.
        /// </summary>
        public long FirstSeqNr { get; }

        /// <summary>
        ///     The unconfirmed messages that have been sent to the consumer.
        /// </summary>
        public ImmutableList<ConsumerController.SequencedMessage<T>> Unconfirmed { get; }

        /// <summary>
        ///     When chunked delivery is enabled, this is where the not-yet-transmitted chunks are stored.
        /// </summary>
        public ImmutableList<ConsumerController.SequencedMessage<T>> RemainingChunks { get; }

        /// <summary>
        ///     A reference to the producer actor.
        /// </summary>
        public IActorRef? Producer { get; }

        // copy state with new producer
        public State WithProducer(IActorRef producer)
        {
            return new State(Requested, CurrentSeqNr, ConfirmedSeqNr, RequestedSeqNr, SupportResend,
                FirstSeqNr, Unconfirmed, producer, RemainingChunks, ReplyAfterStore);
        }

        // copy state with new requested sequence number
        public State WithRequestedSeqNr(long requestedSeqNr)
        {
            return new State(Requested, CurrentSeqNr, ConfirmedSeqNr,
                requestedSeqNr, SupportResend, FirstSeqNr, Unconfirmed, Producer, RemainingChunks, ReplyAfterStore);
        }

        // copy state with new confirmed sequence number
        public State WithConfirmedSeqNr(long confirmedSeqNr)
        {
            return new State(Requested, CurrentSeqNr, confirmedSeqNr,
                RequestedSeqNr, SupportResend, FirstSeqNr, Unconfirmed, Producer, RemainingChunks, ReplyAfterStore);
        }

        // copy state with new current sequence number
        public State WithCurrentSeqNr(long currentSeqNr)
        {
            return new State(Requested, currentSeqNr, ConfirmedSeqNr, RequestedSeqNr, SupportResend,
                FirstSeqNr, Unconfirmed, Producer, RemainingChunks, ReplyAfterStore);
        }

        // copy state with new unconfirmed messages
        public State WithUnconfirmed(ImmutableList<ConsumerController.SequencedMessage<T>> unconfirmed)
        {
            return new State(Requested, CurrentSeqNr, ConfirmedSeqNr, RequestedSeqNr, SupportResend, FirstSeqNr,
                unconfirmed, Producer,
                RemainingChunks, ReplyAfterStore);
        }

        // copy state with new requested flag
        public State WithRequested(bool requested)
        {
            return new State(requested, CurrentSeqNr, ConfirmedSeqNr, RequestedSeqNr, SupportResend,
                FirstSeqNr, Unconfirmed, Producer, RemainingChunks, ReplyAfterStore);
        }

        // copy state with new remaining chunks
        public State WithRemainingChunks(ImmutableList<ConsumerController.SequencedMessage<T>> remainingChunks)
        {
            return new State(Requested, CurrentSeqNr, ConfirmedSeqNr, RequestedSeqNr, SupportResend, FirstSeqNr,
                Unconfirmed, Producer,
                remainingChunks, ReplyAfterStore);
        }

        // copy state with new reply after store
        public State WithReplyAfterStore(ImmutableDictionary<long, IActorRef> replyAfterStore)
        {
            return new State(Requested, CurrentSeqNr, ConfirmedSeqNr, RequestedSeqNr, SupportResend, FirstSeqNr,
                Unconfirmed, Producer,
                RemainingChunks, replyAfterStore);
        }

        // copy state with new support resend
        public State WithSupportResend(bool supportResend)
        {
            return new State(Requested, CurrentSeqNr, ConfirmedSeqNr, RequestedSeqNr, supportResend, FirstSeqNr,
                Unconfirmed, Producer,
                RemainingChunks, ReplyAfterStore);
        }
    }

    /// <summary>
    ///     Send the first message with the lowest delivery id.
    /// </summary>
    public sealed class ResendFirst
    {
        public static readonly ResendFirst Instance = new();

        private ResendFirst()
        {
        }
    }

    #endregion

    #region Behaviors

    private void WaitingForActivation()
    {
        var initialState = CreateInitialState(_durableProducerQueueProps.HasValue);
        
        bool IsReadyForActivation() =>
        !CurrentState.Producer.IsNobody() && !_consumerController.IsNobody() && initialState.HasValue;
        
        Receive<ProducerController.Start<T>>(start =>
        {
            ProducerController.AssertLocalProducer(start.Producer);
            CurrentState = CurrentState.WithProducer(start.Producer);

            // send ChannelWriter<T> to producer
            CurrentState.Producer.Tell(new ProducerController.StartProduction<T>(ProducerId, _channel.Writer));

            if (IsReadyForActivation()) BecomeActive(initialState.Value);
        });

        Receive<ProducerController.RegisterConsumer<T>>(consumer =>
        {
            _consumerController = consumer.ConsumerController;

            if (IsReadyForActivation()) BecomeActive(initialState.Value);
        });
        
        Receive<ProducerController.LoadStateReply<T>>(reply =>
        {
            initialState = reply.State;
            if (IsReadyForActivation()) BecomeActive(initialState.Value);
        });

        Receive<ProducerController.LoadStateFailed>(failed =>
        {
            if (failed.Attempts > Settings.DurableQueueAttemptRetries)
            {
                var errorMessage = $"LoadState failed after [{failed.Attempts}] attempts. Giving up.";
                _log.Error(errorMessage);
                throw new TimeoutException(errorMessage);
            }
            else
            {
                _log.Warning("LoadState failed, attempt [{0}] of [{1}], retrying", failed.Attempts, Settings.DurableQueueAttemptRetries);
                // retry
                AskLoadState(DurableProducerQueueRef, failed.Attempts + 1);
            }
        });
        
        Receive<ProducerController.DurableQueueTerminated>(terminated =>
        {
            throw new IllegalStateException("DurableQueue was unexpectedly terminated.");
        });
    }

    private void BecomeActive(DurableProducerQueue.State<T> state)
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

        CurrentState = CurrentState.WithRequested(requested);

        Become(Active);
    }

    private void Active()
    {
        Receive<ProducerController.SendNext<T>>(sendNext => { });

        Receive<ResendFirst>(_ => ResendFirstMsg());

        Receive<T>(msg => { });
    }

    protected override void PreStart()
    {
        DurableProducerQueueRef = AskLoadState();
    }

    protected override void PostStop()
    {
        // terminate any in-flight requests
        _shutdownCancellation.Cancel();
        base.PostStop();
    }

    #endregion

    #region Internal Methods and Properties
    
    private void CheckOnMsgRequestedState()
    {
        if (!CurrentState.Requested || CurrentState.CurrentSeqNr > CurrentState.RequestedSeqNr)
            throw new IllegalStateException(
                $"Unexpected Msg when no demand, requested: {CurrentState.Requested}, requestedSeqNr: {CurrentState.RequestedSeqNr}, currentSeqNr:{CurrentState.CurrentSeqNr}");
    }

    private void CheckReceiveMessageRemainingChunkState()
    {
        if (CurrentState.RemainingChunks.Any())
            throw new IllegalStateException(
                $"Received unexpected message before sending remaining {CurrentState.RemainingChunks.Count} chunks");
    }

    private Option<IActorRef> AskLoadState()
    {
        return _durableProducerQueueProps.Select(p =>
        {
            var actor = Context.ActorOf(p.WithDispatcher(Context.Dispatcher.Id), "durable");
            Context.WatchWith(actor, ProducerController.DurableQueueTerminated.Instance);
            AskLoadState(Option<IActorRef>.Create(actor), 1);
            return actor;
        });
    }

    private void AskLoadState(Option<IActorRef> durableProducerQueue, int attempt)
    {
        var timeout = Settings.DurableQueueRequestTimeout;
        durableProducerQueue.OnSuccess(@ref =>
        {
            object Mapper(IActorRef r) => new DurableProducerQueue.LoadState<T>(r);

            var self = Self;
            @ref.Ask<DurableProducerQueue.State<T>>((Func<IActorRef, object>)Mapper, timeout: timeout)
                .PipeTo(self, success: state => new ProducerController.LoadStateReply<T>(state),
                    failure: ex => new ProducerController.LoadStateFailed(attempt)); // timeout
        });
    }

    private Option<DurableProducerQueue.State<T>> CreateInitialState(bool hasDurableQueue)
    {
        if (hasDurableQueue) return Option<DurableProducerQueue.State<T>>.None;
        return Option<DurableProducerQueue.State<T>>.Create(DurableProducerQueue.State<T>.Empty);
    }

    private State CreateState(IActorRef producer, DurableProducerQueue.State<T> loadedState)
    {
        var unconfirmedBuilder = ImmutableList.CreateBuilder<ConsumerController.SequencedMessage<T>>();
        var i = 0;
        foreach (var u in loadedState.Unconfirmed)
        {
            unconfirmedBuilder.Add(
                new ConsumerController.SequencedMessage<T>(ProducerId, u.SeqNo, u.Message, i == 0, u.Ack));
            i++;
        }

        return new State(false, loadedState.CurrentSeqNo, loadedState.HighestConfirmedSeqNo, 1L, true,
            loadedState.HighestConfirmedSeqNo + 1, unconfirmedBuilder.ToImmutable(), producer,
            ImmutableList<ConsumerController.SequencedMessage<T>>.Empty, ImmutableDictionary<long, IActorRef>.Empty);
    }

    private void ResendFirstMsg()
    {
        if (!CurrentState.Unconfirmed.IsEmpty && CurrentState.Unconfirmed[0].SeqNr == CurrentState.FirstSeqNr)
        {
            _log.Debug("Resending first message [{0}]", CurrentState.Unconfirmed[0].SeqNr);
            _consumerController.Tell(_sendAdapter(CurrentState.Unconfirmed[0]));
        }
        else
        {
            if (CurrentState.CurrentSeqNr > CurrentState.FirstSeqNr)
                Timers.Cancel(ResendFirst.Instance);
        }
    }

    private ImmutableList<ConsumerController.SequencedMessage<T>> Chunk(T msg, bool ack, Serialization serialization)
    {
        var chunkSize = Settings.ChunkLargeMessagesBytes ?? 0;
        if (chunkSize == 0) // chunking not enabled
        {
            var sequencedMessage = new ConsumerController.SequencedMessage<T>(ProducerId, CurrentState.CurrentSeqNr,
                msg, CurrentState.CurrentSeqNr == CurrentState.FirstSeqNr, ack);
            return ImmutableList<ConsumerController.SequencedMessage<T>>.Empty.Add(sequencedMessage);
        }

        // chunking is enabled
        var chunkedMessages = CreateChunks(msg, chunkSize, _serialization.Value).ToList();
        if (_log.IsDebugEnabled)
        {
            if (chunkedMessages.Count == 1)
                _log.Debug("No chunking of SeqNo [{0}], size [{1}] bytes", CurrentState.CurrentSeqNr,
                    chunkedMessages.First().SerializedMessage.Count);
            else
                _log.Debug("Chunking SeqNo [{0}] into [{1}] chunks, total size [{2}] bytes",
                    CurrentState.CurrentSeqNr, chunkedMessages.Count,
                    chunkedMessages.Sum(x => x.SerializedMessage.Count));
        }

        var i = 0;
        var chunks = chunkedMessages.Select(chunkedMessage =>
        {
            var seqNr = CurrentState.CurrentSeqNr + i;
            i += 1;
            var sequencedMessage = ConsumerController.SequencedMessage<T>.FromChunkedMessage(ProducerId, seqNr,
                chunkedMessage,
                seqNr == CurrentState.FirstSeqNr, ack);
            return sequencedMessage;
        }).ToImmutableList();

        return chunks;
    }

    private static IEnumerable<ChunkedMessage> CreateChunks(T msg, int chunkSize, Serialization serialization)
    {
        var serializer = serialization.FindSerializerForType(typeof(T));
        var manifest = Serialization.ManifestFor(serializer, msg);
        var serializerId = serializer.Identifier;
        var bytes = serialization.Serialize(msg);
        if (bytes.Length <= chunkSize)
        {
            var chunkedMessage = new ChunkedMessage(ByteString.CopyFrom(bytes), true, true, serializerId, manifest);
            yield return chunkedMessage;
        }
        else
        {
            var chunkCount = (int)Math.Ceiling(bytes.Length / (double)chunkSize);
            var first = true;
            for (var i = 0; i < chunkCount; i++)
            {
                var isLast = i == chunkCount - 1;
                var chunkedMessage = new ChunkedMessage(ByteString.CopyFrom(bytes, i * chunkSize, chunkSize), first,
                    isLast, serializerId, manifest);

                first = false;
                yield return chunkedMessage;
            }
        }
    }

    #endregion
}