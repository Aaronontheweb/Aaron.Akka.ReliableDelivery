// -----------------------------------------------------------------------
//  <copyright file="ProducerControllerImpl.cs" company="Akka.NET Project">
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
using Akka.Actor;
using Akka.Event;
using Akka.IO;
using Akka.Pattern;
using Akka.Serialization;
using Akka.Util;
using static Aaron.Akka.ReliableDelivery.ProducerController;
using static Aaron.Akka.ReliableDelivery.ConsumerController;

namespace Aaron.Akka.ReliableDelivery.Internal;

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
    private readonly Option<Props> _durableProducerQueueProps;
    private readonly ITimeProvider _timeProvider;

    public ProducerController(string producerId, ProducerController.Settings settings,
        Option<Props> durableProducerQueue, ITimeProvider? timeProvider = null,
        Func<SequencedMessage<T>, object>? sendAdapter = null)
    {
        ProducerId = producerId;
        Settings = settings;
        _durableProducerQueueProps = durableProducerQueue;
        _timeProvider = timeProvider ?? DateTimeOffsetNowTimeProvider.Instance;
        _sendAdapter = sendAdapter ?? DefaultSend;
        _channel = Channel.CreateBounded<ProducerController.SendNext<T>>(
            new BoundedChannelOptions(Settings.DeliveryBufferSize)
            {
                SingleWriter = true, SingleReader = true, FullMode = BoundedChannelFullMode.Wait
            }); // force busy producers to wait

        // this state gets overridden during the loading sequence, so it's not used at all really
        CurrentState = new State(false, 0, 0, 0, true, 0, ImmutableList<ConsumerController.SequencedMessage<T>>.Empty,
            ActorRefs.NoSender, ImmutableList<SequencedMessage<T>>.Empty,
            ImmutableDictionary<long, IActorRef>.Empty, _ => { }, 0);

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
            ImmutableList<SequencedMessage<T>> unconfirmed, IActorRef? producer,
            ImmutableList<SequencedMessage<T>> remainingChunks,
            ImmutableDictionary<long, IActorRef> replyAfterStore, Action<SequencedMessage<T>> send,
            long storeMessageSentInProgress)
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
            Send = send;
            StoreMessageSentInProgress = storeMessageSentInProgress;
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
        public ImmutableList<SequencedMessage<T>> Unconfirmed { get; }

        /// <summary>
        ///     When chunked delivery is enabled, this is where the not-yet-transmitted chunks are stored.
        /// </summary>
        public ImmutableList<SequencedMessage<T>> RemainingChunks { get; }

        /// <summary>
        /// The sequence number of a message that is currently being stored.
        /// </summary>
        public long StoreMessageSentInProgress { get; }

        /// <summary>
        ///     A reference to the producer actor.
        /// </summary>
        public IActorRef? Producer { get; }

        /// <summary>
        /// Monad for sending a message to the ConsumerController.
        /// </summary>
        public Action<SequencedMessage<T>> Send { get; }

        // copy state with new producer
        public State WithProducer(IActorRef producer)
        {
            return new State(Requested, CurrentSeqNr, ConfirmedSeqNr, RequestedSeqNr, SupportResend,
                FirstSeqNr, Unconfirmed, producer, RemainingChunks, ReplyAfterStore, Send, StoreMessageSentInProgress);
        }

        // copy state with new requested sequence number
        public State WithRequestedSeqNr(long requestedSeqNr)
        {
            return new State(Requested, CurrentSeqNr, ConfirmedSeqNr,
                requestedSeqNr, SupportResend, FirstSeqNr, Unconfirmed, Producer, RemainingChunks, ReplyAfterStore,
                Send, StoreMessageSentInProgress);
        }

        // copy state with new confirmed sequence number
        public State WithConfirmedSeqNr(long confirmedSeqNr)
        {
            return new State(Requested, CurrentSeqNr, confirmedSeqNr,
                RequestedSeqNr, SupportResend, FirstSeqNr, Unconfirmed, Producer, RemainingChunks, ReplyAfterStore,
                Send, StoreMessageSentInProgress);
        }

        // copy state with new current sequence number
        public State WithCurrentSeqNr(long currentSeqNr)
        {
            return new State(Requested, currentSeqNr, ConfirmedSeqNr, RequestedSeqNr, SupportResend,
                FirstSeqNr, Unconfirmed, Producer, RemainingChunks, ReplyAfterStore, Send, StoreMessageSentInProgress);
        }

        // copy state with new unconfirmed messages
        public State WithUnconfirmed(ImmutableList<SequencedMessage<T>> unconfirmed)
        {
            return new State(Requested, CurrentSeqNr, ConfirmedSeqNr, RequestedSeqNr, SupportResend, FirstSeqNr,
                unconfirmed, Producer,
                RemainingChunks, ReplyAfterStore, Send, StoreMessageSentInProgress);
        }

        // copy state with new requested flag
        public State WithRequested(bool requested)
        {
            return new State(requested, CurrentSeqNr, ConfirmedSeqNr, RequestedSeqNr, SupportResend,
                FirstSeqNr, Unconfirmed, Producer, RemainingChunks, ReplyAfterStore, Send, StoreMessageSentInProgress);
        }

        // copy state with new remaining chunks
        public State WithRemainingChunks(ImmutableList<SequencedMessage<T>> remainingChunks)
        {
            return new State(Requested, CurrentSeqNr, ConfirmedSeqNr, RequestedSeqNr, SupportResend, FirstSeqNr,
                Unconfirmed, Producer,
                remainingChunks, ReplyAfterStore, Send, StoreMessageSentInProgress);
        }

        // copy state with new reply after store
        public State WithReplyAfterStore(ImmutableDictionary<long, IActorRef> replyAfterStore)
        {
            return new State(Requested, CurrentSeqNr, ConfirmedSeqNr, RequestedSeqNr, SupportResend, FirstSeqNr,
                Unconfirmed, Producer,
                RemainingChunks, replyAfterStore, Send, StoreMessageSentInProgress);
        }

        // copy state with new support resend
        public State WithSupportResend(bool supportResend)
        {
            return new State(Requested, CurrentSeqNr, ConfirmedSeqNr, RequestedSeqNr, supportResend, FirstSeqNr,
                Unconfirmed, Producer,
                RemainingChunks, ReplyAfterStore, Send, StoreMessageSentInProgress);
        }

        // copy state with new send function
        public State WithSend(Action<SequencedMessage<T>> send)
        {
            return new State(Requested, CurrentSeqNr, ConfirmedSeqNr, RequestedSeqNr, SupportResend, FirstSeqNr,
                Unconfirmed, Producer,
                RemainingChunks, ReplyAfterStore, send, StoreMessageSentInProgress);
        }

        // copy state with new store message sent in progress
        public State WithStoreMessageSentInProgress(long storeMessageSentInProgress)
        {
            return new State(Requested, CurrentSeqNr, ConfirmedSeqNr, RequestedSeqNr, SupportResend, FirstSeqNr,
                Unconfirmed, Producer,
                RemainingChunks, ReplyAfterStore, Send, storeMessageSentInProgress);
        }
    }

    #endregion

    #region Behaviors

    private void WaitingForActivation()
    {
        var initialState = CreateInitialState(_durableProducerQueueProps.HasValue);
        var consumerController = ActorRefs.NoSender;

        bool IsReadyForActivation() =>
            !CurrentState.Producer.IsNobody() && !consumerController.IsNobody() && initialState.HasValue;

        Receive<ProducerController.Start<T>>(start =>
        {
            AssertLocalProducer(start.Producer);
            CurrentState = CurrentState.WithProducer(start.Producer);

            // send ChannelWriter<T> to producer
            CurrentState.Producer.Tell(new StartProduction<T>(ProducerId, _channel.Writer));

            if (IsReadyForActivation())
                BecomeActive(CreateState(start.Producer, consumerController, initialState.Value));
        });

        Receive<RegisterConsumer<T>>(consumer =>
        {
            consumerController = consumer.ConsumerController;

            if (IsReadyForActivation())
                BecomeActive(CreateState(CurrentState.Producer!, consumerController, initialState.Value));
        });

        Receive<LoadStateReply<T>>(reply =>
        {
            initialState = reply.State;
            if (IsReadyForActivation())
                BecomeActive(CreateState(CurrentState.Producer!, consumerController, initialState.Value));
        });

        Receive<LoadStateFailed>(failed =>
        {
            if (failed.Attempts > Settings.DurableQueueAttemptRetries)
            {
                var errorMessage = $"LoadState failed after [{failed.Attempts}] attempts. Giving up.";
                _log.Error(errorMessage);
                throw new TimeoutException(errorMessage);
            }
            else
            {
                _log.Warning("LoadState failed, attempt [{0}] of [{1}], retrying", failed.Attempts,
                    Settings.DurableQueueAttemptRetries);
                // retry
                AskLoadState(DurableProducerQueueRef, failed.Attempts + 1);
            }
        });

        Receive<DurableQueueTerminated>(terminated =>
        {
            throw new IllegalStateException("DurableQueue was unexpectedly terminated.");
        });
    }

    private void BecomeActive(State state)
    {
        CurrentState = state;
        var requested = false;
        if (CurrentState.Unconfirmed.IsEmpty)
        {
            requested = true;

            ReadNext();
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

    private void ReadNext()
    {
        // kick off read task
        var self = Self;
        _channel.Reader.ReadAsync(ShutdownToken).PipeTo(self);
    }

    private void Active()
    {
        // receiving a live message with an explicit ACK target
        Receive<SendNext<T>>(next => !next.SendConfirmationTo.IsNobody(), sendNext =>
        {
            CheckReceiveMessageRemainingChunkState();
            var chunks = Chunk(sendNext.Message, true, _serialization.Value);
            var newReplyAfterStore =
                CurrentState.ReplyAfterStore.SetItem(chunks.Last().SeqNr, sendNext.SendConfirmationTo!);

            if (DurableProducerQueueRef.IsEmpty)
            {
            }
        });

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

    private void OnMsg(SequencedMessage<T> seqMsg,
        ImmutableDictionary<long, IActorRef> newReplyAfterStore,
        ImmutableList<SequencedMessage<T>> remainingChunks)
    {
        CheckOnMsgRequestedState();
        if (seqMsg.IsLastChunk && remainingChunks.Any())
            throw new IllegalStateException(
                $"SeqNsg [{seqMsg.SeqNr}] was last chunk but remaining {remainingChunks.Count} chunks");

        if (_log.IsDebugEnabled)
        {
            _log.Debug("Sending [{0}] with seqNo [{1}] to consumer", seqMsg.Message, seqMsg.SeqNr);
        }

        var newUnconfirmed = CurrentState.SupportResend
            ? CurrentState.Unconfirmed.Add(seqMsg)
            : ImmutableList<SequencedMessage<T>>.Empty; // no need to keep unconfirmed if no resend

        if (CurrentState.CurrentSeqNr == CurrentState.FirstSeqNr)
            Timers.StartSingleTimer(ResendFirst.Instance, ResendFirst.Instance,
                Settings.DurableQueueResendFirstInterval);

        CurrentState.Send(seqMsg);
        var newRequested = false;
        if (CurrentState.CurrentSeqNr == CurrentState.RequestedSeqNr)
        {
            newRequested = remainingChunks.Any();
        }
        else if (seqMsg.IsLastChunk)
        {
            // kick off read task
            ReadNext();
            newRequested = true;
        }
        else
        {
            Self.Tell(SendChunk.Instance);
            newRequested = true;
        }

        CurrentState = CurrentState.WithUnconfirmed(newUnconfirmed)
            .WithRequested(newRequested)
            .WithCurrentSeqNr(CurrentState.CurrentSeqNr + 1)
            .WithReplyAfterStore(newReplyAfterStore)
            .WithRemainingChunks(remainingChunks)
            .WithStoreMessageSentInProgress(0);
    }

    private State OnAck(long newConfirmedSeqNr)
    {
        var replies = CurrentState.ReplyAfterStore.Where(c => c.Key <= newConfirmedSeqNr).Select(c => (c.Key, c.Value))
            .ToArray();
        var newReplyAfterStore =
            CurrentState.ReplyAfterStore.Where(c => c.Key > newConfirmedSeqNr).ToImmutableDictionary();

        if (_log.IsDebugEnabled && replies.Any())
        {
            _log.Debug("Sending confirmation replies from [{0}] to [{1}]", replies.First().Key, replies.Last().Key);
        }

        foreach (var (seqNr, replyTo) in replies)
        {
            replyTo.Tell(seqNr);
        }

        var newUnconfirmed = CurrentState.SupportResend
            ? CurrentState.Unconfirmed.Where(c => c.SeqNr > newConfirmedSeqNr).ToImmutableList()
            : ImmutableList<SequencedMessage<T>>.Empty; // no need to keep unconfirmed if no resend

        if (newConfirmedSeqNr == CurrentState.FirstSeqNr)
            Timers.Cancel(ResendFirst.Instance);

        var newMaxConfirmedSeqNr = Math.Max(CurrentState.ConfirmedSeqNr, newConfirmedSeqNr);

        DurableProducerQueueRef.OnSuccess(d =>
        {
            // Storing the confirmedSeqNr can be "write behind", at-least-once delivery
            // TODO to reduce number of writes, consider to only StoreMessageConfirmed for the Request messages and not for each Ack
            if (newMaxConfirmedSeqNr != CurrentState.ConfirmedSeqNr)
                d.Tell(new DurableProducerQueue.StoreMessageConfirmed<T>(newMaxConfirmedSeqNr,
                    DurableProducerQueue.NoQualifier, _timeProvider.Now.Ticks));
        });

        return CurrentState.WithConfirmedSeqNr(newMaxConfirmedSeqNr).WithReplyAfterStore(newReplyAfterStore)
            .WithUnconfirmed(newUnconfirmed);
    }

    private void ReceiveRequest(long newConfirmedSeqNr, long newRequestedSeqNr, bool supportResend, bool viaTimeout)
    {
        _log.Debug("Received request, confirmed [{0}], requested [{1}], current [{2}]", newConfirmedSeqNr,
            newRequestedSeqNr, CurrentState.CurrentSeqNr);

        var stateAfterAck = OnAck(newConfirmedSeqNr);

        var newUnconfirmed = supportResend ? stateAfterAck.Unconfirmed : ImmutableList<SequencedMessage<T>>.Empty;

        if ((viaTimeout || newConfirmedSeqNr == CurrentState.FirstSeqNr) && supportResend)
        {
            // the last message was lost and no more message was sent that would trigger Resend
            ResendUnconfirmed(newUnconfirmed);
        }

        // when supportResend=false the requestedSeqNr window must be expanded if all sent messages were lost
        var newRequestedSeqNr2 = (!supportResend && newRequestedSeqNr <= stateAfterAck.CurrentSeqNr)
            ? stateAfterAck.CurrentSeqNr + (newRequestedSeqNr - newConfirmedSeqNr)
            : newRequestedSeqNr;

        if (newRequestedSeqNr2 != newRequestedSeqNr)
            _log.Debug("Expanded requestedSeqNr from [{0}] to [{1}], because current [{3}] and all were probably lost.",
                newRequestedSeqNr, newRequestedSeqNr2, stateAfterAck.CurrentSeqNr);

        if (newRequestedSeqNr > CurrentState.RequestedSeqNr)
        {
            bool newRequested;
            if (CurrentState.StoreMessageSentInProgress != 0)
            {
                newRequested = CurrentState.Requested;
            } else if (!CurrentState.RemainingChunks.IsEmpty)
            {
                Self.Tell(SendChunk.Instance);
                newRequested = CurrentState.Requested;
            } else if (!CurrentState.Requested && (newRequestedSeqNr2 - CurrentState.RequestedSeqNr) > 0)
            {
                ReadNext();
                newRequested = true;
            }
            else
            {
                newRequested = CurrentState.Requested;
            }

            CurrentState = CurrentState.WithRequested(newRequested)
                .WithSupportResend(supportResend)
                .WithRequestedSeqNr(newRequestedSeqNr2)
                .WithUnconfirmed(newUnconfirmed);
        }
        else
        {
            CurrentState = CurrentState.WithSupportResend(supportResend)
                .WithUnconfirmed(newUnconfirmed);
        }
    }

    private void ReceiveAck(long newConfirmedSeqNr)
    {
        if (_log.IsDebugEnabled)
        {
            _log.Debug("Received ack, confirmed [{0}], current [{1}]", newConfirmedSeqNr, CurrentState.CurrentSeqNr);
        }

        var stateAfterAck = OnAck(newConfirmedSeqNr);
        if (newConfirmedSeqNr == stateAfterAck.FirstSeqNr && !stateAfterAck.Unconfirmed.IsEmpty)
        {
            ResendUnconfirmed(stateAfterAck.Unconfirmed);
        }
        
        CurrentState = stateAfterAck;
    }

    private Option<IActorRef> AskLoadState()
    {
        return _durableProducerQueueProps.Select(p =>
        {
            var actor = Context.ActorOf(p.WithDispatcher(Context.Dispatcher.Id), "durable");
            Context.WatchWith(actor, DurableQueueTerminated.Instance);
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
                .PipeTo(self, success: state => new LoadStateReply<T>(state),
                    failure: ex => new LoadStateFailed(attempt)); // timeout
        });
    }

    private Option<DurableProducerQueue.State<T>> CreateInitialState(bool hasDurableQueue)
    {
        if (hasDurableQueue) return Option<DurableProducerQueue.State<T>>.None;
        return Option<DurableProducerQueue.State<T>>.Create(DurableProducerQueue.State<T>.Empty);
    }

    private State CreateState(IActorRef producer, IActorRef consumerController,
        DurableProducerQueue.State<T> loadedState)
    {
        var unconfirmedBuilder = ImmutableList.CreateBuilder<ConsumerController.SequencedMessage<T>>();
        var i = 0;
        foreach (var u in loadedState.Unconfirmed)
        {
            unconfirmedBuilder.Add(
                new SequencedMessage<T>(ProducerId, u.SeqNo, u.Message, i == 0, u.Ack));
            i++;
        }

        void Send(SequencedMessage<T> msg)
        {
            consumerController.Tell(msg);
        }

        return new State(false, loadedState.CurrentSeqNo, loadedState.HighestConfirmedSeqNo, 1L, true,
            loadedState.HighestConfirmedSeqNo + 1, unconfirmedBuilder.ToImmutable(), producer,
            ImmutableList<SequencedMessage<T>>.Empty, ImmutableDictionary<long, IActorRef>.Empty, Send, 0);
    }

    private void ResendFirstMsg()
    {
        if (!CurrentState.Unconfirmed.IsEmpty && CurrentState.Unconfirmed[0].SeqNr == CurrentState.FirstSeqNr)
        {
            _log.Debug("Resending first message [{0}]", CurrentState.Unconfirmed[0].SeqNr);
            CurrentState.Send(CurrentState.Unconfirmed[0]);
        }
        else
        {
            if (CurrentState.CurrentSeqNr > CurrentState.FirstSeqNr)
                Timers.Cancel(ResendFirst.Instance);
        }
    }

    private void ResendUnconfirmed(ImmutableList<SequencedMessage<T>> newUnconfirmed)
    {
        if (!newUnconfirmed.IsEmpty)
        {
            var fromSeqNr = newUnconfirmed.First().SeqNr;
            var toSeqNr = newUnconfirmed.Last().SeqNr;
            _log.Debug("Resending unconfirmed messages [{0} - {1}]", fromSeqNr, toSeqNr);
            foreach (var n in newUnconfirmed)
                CurrentState.Send(n);
        }
    }

    private void ReceiveResendFirstUnconfirmed()
    {
        if (CurrentState.Unconfirmed.IsEmpty)
        {
            _log.Debug("Resending first unconfirmed [{0}]", CurrentState.Unconfirmed.First().SeqNr);
            CurrentState.Send(CurrentState.Unconfirmed.First());
        }
    }

    private void ReceiveResendFirst()
    {
        if (!CurrentState.Unconfirmed.IsEmpty && CurrentState.Unconfirmed[0].SeqNr == CurrentState.FirstSeqNr)
        {
            _log.Debug("Resending first message [{0}]", CurrentState.Unconfirmed[0].SeqNr);
            CurrentState.Send(CurrentState.Unconfirmed[0]);
        }
    }

    private void ReceiveStart(ProducerController.Start<T> start)
    {
        AssertLocalProducer(start.Producer);
        _log.Debug("Registering new producer [{0}], currentSeqNr [{1}]", start.Producer, CurrentState.CurrentSeqNr);
        if (CurrentState is { Requested: true, RemainingChunks.IsEmpty: true })
        {
            ReadNext();
        }

        CurrentState = CurrentState.WithProducer(start.Producer);
    }

    private void ReceiveRegisterConsumer(IActorRef consumerController)
    {
        var newFirstSeqNr = CurrentState.Unconfirmed.IsEmpty
            ? CurrentState.CurrentSeqNr
            : CurrentState.Unconfirmed.First().SeqNr;

        _log.Debug("Register new ConsumerController [{0}], starting with seqNr [{1}]", consumerController,
            newFirstSeqNr);

        if (!CurrentState.Unconfirmed.IsEmpty)
        {
            Timers.StartSingleTimer(ResendFirst.Instance, ResendFirst.Instance,
                Settings.DurableQueueResendFirstInterval);
            Self.Tell(ResendFirst.Instance);
        }

        // update the send function
        void Send(SequencedMessage<T> msg)
        {
            consumerController.Tell(msg);
        }

        CurrentState = CurrentState.WithSend(Send);
    }

    private void ReceiveSendChunk()
    {
        if (!CurrentState.RemainingChunks.IsEmpty &&
            CurrentState.RemainingChunks.First().SeqNr <= CurrentState.RequestedSeqNr &&
            CurrentState.StoreMessageSentInProgress == 0)
        {
            if (_log.IsDebugEnabled)
            {
                _log.Debug("Send next chunk seqNr [{0}].", CurrentState.RemainingChunks.First().SeqNr);
            }

            if (DurableProducerQueueRef.IsEmpty)
            {
                // TODO: optimize this using spans and slices
                OnMsg(CurrentState.RemainingChunks.First(), CurrentState.ReplyAfterStore,
                    CurrentState.RemainingChunks.Skip(1).ToImmutableList());
            }
            else
            {
                var seqMsg = CurrentState.RemainingChunks.First();
                StoreMessageSent(
                    DurableProducerQueue.MessageSent<T>.FromMessageOrChunked(seqMsg.SeqNr, seqMsg.Message, seqMsg.Ack,
                        DurableProducerQueue.NoQualifier, _timeProvider.Now.Ticks), 1);
                CurrentState = CurrentState.WithStoreMessageSentInProgress(seqMsg.SeqNr);
            }
        }
    }

    private ImmutableList<SequencedMessage<T>> Chunk(T msg, bool ack, Serialization serialization)
    {
        var chunkSize = Settings.ChunkLargeMessagesBytes ?? 0;
        if (chunkSize == 0) // chunking not enabled
        {
            var sequencedMessage = new SequencedMessage<T>(ProducerId, CurrentState.CurrentSeqNr,
                msg, CurrentState.CurrentSeqNr == CurrentState.FirstSeqNr, ack);
            return ImmutableList<SequencedMessage<T>>.Empty.Add(sequencedMessage);
        }

        // chunking is enabled
        var chunkedMessages = CreateChunks(msg, chunkSize, serialization).ToList();
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
            var sequencedMessage = SequencedMessage<T>.FromChunkedMessage(ProducerId, seqNr,
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

    private void StoreMessageSent(DurableProducerQueue.MessageSent<T> messageSent, int attempt)
    {
        object Mapper(IActorRef r) => new DurableProducerQueue.StoreMessageSent<T>(messageSent, r);

        var self = Self;
        DurableProducerQueueRef.Value.Ask<DurableProducerQueue.StoreMessageSentAck>((Func<IActorRef, object>)Mapper,
                Settings.DurableQueueRequestTimeout)
            .PipeTo(self, success: ack => new StoreMessageSentCompleted<T>(messageSent),
                failure: ex => new StoreMessageSentFailed<T>(messageSent, attempt));
    }

    #endregion
}