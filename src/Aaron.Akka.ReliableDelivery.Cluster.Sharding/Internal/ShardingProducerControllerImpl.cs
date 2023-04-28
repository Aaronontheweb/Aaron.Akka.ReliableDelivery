using System.Collections.Immutable;
using Aaron.Akka.ReliableDelivery.Internal;
using Akka.Actor;
using Akka.Actor.Dsl;
using Akka.Cluster.Sharding;
using Akka.Event;
using Akka.Pattern;
using Akka.Util;
using Akka.Util.Extensions;
using static Aaron.Akka.ReliableDelivery.Cluster.Sharding.ShardingProducerController;
namespace Aaron.Akka.ReliableDelivery.Cluster.Sharding.Internal;

using OutKey = String;
using EntityId = String;
using TotalSeqNr = Int64;
using OutSeqNr = Int64;

internal sealed class ShardingProducerController<T> : ReceiveActor, IWithStash, IWithTimers
{
    public string ProducerId { get; }

    public IActorRef ShardRegion { get; }

    public ShardingProducerController.Settings Settings { get; }

    public Option<IActorRef> DurableQueueRef { get; private set; } = Option<IActorRef>.None;
    private readonly Option<Props> _durableQueueProps;
    private readonly ITimeProvider _timeProvider;
    
    public IActorRef MsgAdapter { get; private set; } = ActorRefs.Nobody;

    public State<T> CurrentState { get; private set; } = State<T>.Empty;

    private readonly ILoggingAdapter _log = Context.GetLogger();
    public IStash Stash { get; set; } = null!;

    public ITimerScheduler Timers { get; set; } = null!;

    public ShardingProducerController(string producerId, IActorRef shardRegion, Option<Props> durableQueueProps,
        ShardingProducerController.Settings settings, ITimeProvider? timeProvider = null)
    {
        ProducerId = producerId;
        ShardRegion = shardRegion;
        _durableQueueProps = durableQueueProps;
        Settings = settings;
        _timeProvider = timeProvider ?? DateTimeOffsetNowTimeProvider.Instance;

        WaitingForStart(Option<IActorRef>.None, CreateInitialState(_durableQueueProps.HasValue));
    }

    protected override void PreStart()
    {
        DurableQueueRef = AskLoadState();
    }

    #region Behaviors

    private void WaitingForStart(Option<IActorRef> producer, Option<DurableProducerQueue.State<T>> initialState)
    {
        Receive<Start<T>>(start =>
        {
            ProducerController.AssertLocalProducer(start.Producer);
            if (initialState.HasValue)
            {
                BecomeActive(start.Producer, initialState);
            }
            else
            {
                // waiting for load state reply
                producer = start.Producer.AsOption();
            }
        });

        Receive<LoadStateReply<T>>(reply =>
        {
            if (producer.HasValue)
            {
                BecomeActive(producer.Value, reply.State);
            }
            else
            {
                // waiting for Start
                initialState = reply.State;
            }
        });

        Receive<LoadStateFailed>(failed =>
        {
            if (failed.Attempt >= Settings.ProducerControllerSettings.DurableQueueRetryAttempts)
            {
                var errorMsg = $"Failed to load state from durable queue after {failed.Attempt} attempts, giving up.";
                _log.Error(errorMsg);
                throw new TimeoutException(errorMsg);
            }
            else
            {
                _log.Warning("LoadState failed, attempt [{0}] of [{1}], retrying.", failed.Attempt, Settings.ProducerControllerSettings.DurableQueueRetryAttempts);
                // retry
                AskLoadState(DurableQueueRef, failed.Attempt + 1);
            }
        });

        Receive<DurableQueueTerminated>(_ =>
        {
            throw new IllegalStateException("DurableQueue was unexpectedly terminated.");
        });
        
        ReceiveAny(_ =>
        {
            CheckIfStashIsFull();
            Stash.Stash();
        });
    }

    private void BecomeActive(IActorRef producer, Option<DurableProducerQueue.State<T>> initialState)
    {
        Timers.StartPeriodicTimer(CleanupUnused.Instance, CleanupUnused.Instance, TimeSpan.FromMilliseconds(Settings.CleanupUnusedAfter.TotalMilliseconds / 2));
        Timers.StartPeriodicTimer(ResendFirstUnconfirmed.Instance, ResendFirstUnconfirmed.Instance, TimeSpan.FromMilliseconds(Settings.ResendFirstUnconfirmedIdleTimeout.TotalMilliseconds / 2));
        
        // resend unconfirmed before other stashed messages
        initialState.OnSuccess(s =>
        {
            var unconfirmedDeliveries = s.Unconfirmed.Select(c => new Envelope(c, Self));
            Stash.Prepend(unconfirmedDeliveries);
        });

        var self = Self;
        MsgAdapter = Context.ActorOf(act =>
        {
            act.Receive<ShardingEnvelope>((msg, ctx) =>
            {
                self.Forward(new Msg(msg, 0));
            });
            
            act.ReceiveAny((_, ctx) =>
            {
                var errorMessage =
                    $"Message sent to ShardingProducerController must be ShardingEnvelope, was [{_.GetType()
                        .Name}]";
                ctx.GetLogger().Error(errorMessage);
                ctx.Sender.Tell(new Status.Failure(new InvalidOperationException(errorMessage)));
            });
        }, "msg-adapter");
        
        if(initialState.IsEmpty || initialState.Value.Unconfirmed.IsEmpty)
            producer.Tell(new RequestNext<T>(MsgAdapter, Self,ImmutableHashSet<string>.Empty, ImmutableDictionary<string, int>.Empty));
        
        CurrentState = CurrentState with
        {
            Producer = producer,
            CurrentSeqNr = initialState.HasValue ? initialState.Value.CurrentSeqNr : 0,
        };
        
        Become(Active);
        Stash.UnstashAll();
    }

    private void Active()
    {
    }

    #endregion

    #region Internal Methods

    private void OnMsg(EntityId entityId, T msg, Option<IActorRef> replyTo, TotalSeqNr totalSeqNr,
        ImmutableDictionary<TotalSeqNr, IActorRef> newReplyAfterStore)
    {
        var outKey = $"{ProducerId}-{entityId}";
        if (CurrentState.OutStates.TryGetValue(outKey, out var outState))
        {
            // there is demand, send immediately
            if (outState.NextTo.HasValue)
            {
                Send(msg, outKey, outState.SeqNr, outState.NextTo.Value);
                var newUnconfirmed = outState.Unconfirmed.Add(new Unconfirmed<T>(totalSeqNr, outState.SeqNr, replyTo));

                CurrentState = CurrentState with
                {
                    OutStates = CurrentState.OutStates.SetItem(outKey, outState with
                    {
                        SeqNr = outState.SeqNr + 1,
                        Unconfirmed = newUnconfirmed,
                        NextTo = Option<IActorRef>.None,
                        LastUsed = _timeProvider.Now.Ticks
                    }),
                    ReplyAfterStore = newReplyAfterStore
                };
            }
            else
            {
                var buffered = outState.Buffered;
                
                // no demand, buffer
                if(CurrentState.BufferSize >= Settings.BufferSize)
                    throw new IllegalStateException($"Buffer overflow, current size [{CurrentState.BufferSize}] >= max [{Settings.BufferSize}]");
                _log.Debug("Buffering message to entityId [{0}], buffer size for entityId [{1}]", entityId, buffered.Count + 1);
                
                var newBuffered = buffered.Add(new Buffered<T>(totalSeqNr, msg, replyTo));
                var newS = CurrentState with
                {
                    OutStates = CurrentState.OutStates.SetItem(outKey, outState with
                    {
                        Buffered = newBuffered
                    }),
                    ReplyAfterStore = newReplyAfterStore
                };
                // send an updated RequestNext to indicate buffer usage
                CurrentState.Producer.Tell(CreateRequestNext(newS));
                CurrentState = newS;
            }
        }
        else
        {
            _log.Debug("Creating ProducerController for entityId [{0}]", entityId);
            // TODO: need to pass in custom send function for ProducerController in order to map SequencedMessage to ShardingEnvelope
        }
    }

    private RequestNext<T> CreateRequestNext(State<T> state)
    {
        var entitiesWithDemand = state.OutStates.Values.Where(c => c.NextTo.HasValue).Select(c => c.EntityId).ToImmutableHashSet();
        var bufferedForEntitiesWithoutDemand = state.OutStates.Values.Where(c => c.NextTo.IsEmpty)
            .ToImmutableDictionary(c => c.EntityId, c => c.Buffered.Count);
        
        return new RequestNext<T>(MsgAdapter, Self, entitiesWithDemand, bufferedForEntitiesWithoutDemand);
    }

    private void Send(T msg, OutKey outKey, OutSeqNr outSeqNr, IActorRef nextTo)
    {
        if(_log.IsDebugEnabled) // TODO: add trace support
            _log.Debug("Sending [{0}] to [{1}] with outSeqNr [{2}]", msg?.GetType().Name, nextTo, outSeqNr);

        ProducerController.MessageWithConfirmation<T> Transform(IActorRef askTarget)
        {
            return new ProducerController.MessageWithConfirmation<T>(msg, askTarget);
        }

        var self = Self;
        nextTo.Ask<long>(Transform, Settings.InternalAskTimeout, CancellationToken.None)
            .PipeTo(self, success: seqNr =>
                {
                    if (seqNr != outSeqNr)
                        _log.Error("Inconsistent Ack seqNr [{0}] != [{1}]", seqNr, outSeqNr);
                    return new Ack(outKey, seqNr);
                },
                failure: _ => new AskTimeout(outKey, outSeqNr));
    }

    private static Option<DurableProducerQueue.State<T>> CreateInitialState(bool hasDurableQueue)
    {
        return hasDurableQueue ? Option<DurableProducerQueue.State<T>>.None : DurableProducerQueue.State<T>.Empty;
    }

    private void CheckIfStashIsFull()
    {
        if (Stash.IsFull)
            throw new ArgumentException($"Buffer is full, size [{Stash.Count}]");
    }

    private Option<IActorRef> AskLoadState()
    {
        return _durableQueueProps.Select(p =>
        {
            var durableQueue = Context.ActorOf(p, "durable");
            Context.WatchWith(durableQueue, DurableQueueTerminated.Instance);
            AskLoadState(durableQueue.AsOption(), 1);
            return durableQueue;
        });
    }

    private void AskLoadState(Option<IActorRef> durableProducerQueue, int attempt)
    {
        var loadTimeout = Settings.ProducerControllerSettings.DurableQueueRequestTimeout;
        durableProducerQueue.OnSuccess(@ref =>
        {
            DurableProducerQueue.LoadState<T> Mapper(IActorRef r) => new DurableProducerQueue.LoadState<T>(r);

            var self = Self;
            @ref.Ask<DurableProducerQueue.State<T>>(Mapper, timeout: loadTimeout, cancellationToken: default)
                .PipeTo(self, success: state => new LoadStateReply<T>(state),
                    failure: ex => new LoadStateFailed(attempt)); // timeout
        });
    }

    #endregion
}