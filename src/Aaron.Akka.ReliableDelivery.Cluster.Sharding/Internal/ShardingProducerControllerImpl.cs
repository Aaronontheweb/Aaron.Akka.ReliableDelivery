using System.Collections.Immutable;
using Akka.Actor;
using Akka.Actor.Dsl;
using Akka.Cluster.Sharding;
using Akka.Event;
using Akka.Pattern;
using Akka.Util;
using Akka.Util.Extensions;
using static Aaron.Akka.ReliableDelivery.Cluster.Sharding.ShardingProducerController;

namespace Aaron.Akka.ReliableDelivery.Cluster.Sharding.Internal;

internal sealed class ShardingProducerController<T> : ReceiveActor, IWithStash, IWithTimers
{
    public string ProducerId { get; }

    public IActorRef ShardRegion { get; }

    public ShardingProducerController.Settings Settings { get; }

    public Option<IActorRef> DurableQueueRef { get; private set; } = Option<IActorRef>.None;
    private readonly Option<Props> _durableQueueProps;
    
    public IActorRef MsgAdapter { get; private set; } = ActorRefs.Nobody;

    public State<T> CurrentState { get; private set; } = State<T>.Empty;

    private readonly ILoggingAdapter _log = Context.GetLogger();
    public IStash Stash { get; set; } = null!;

    public ITimerScheduler Timers { get; set; } = null!;

    public ShardingProducerController(string producerId, IActorRef shardRegion, Option<Props> durableQueueProps,
        ShardingProducerController.Settings settings)
    {
        ProducerId = producerId;
        ShardRegion = shardRegion;
        _durableQueueProps = durableQueueProps;
        Settings = settings;

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
        
        Become(Active);
        Stash.UnstashAll();
    }

    private void Active()
    {
        Receive<Start<T>>(start =>
        {
            ProducerController.AssertLocalProducer(start.Producer);
            
        });
    }

    #endregion

    #region Internal Methods

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