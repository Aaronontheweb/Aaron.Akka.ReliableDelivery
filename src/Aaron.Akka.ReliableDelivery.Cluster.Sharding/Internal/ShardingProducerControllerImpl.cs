using Akka.Actor;
using Akka.Event;
using Akka.Util;
using Akka.Util.Extensions;
using static Aaron.Akka.ReliableDelivery.Cluster.Sharding.ShardingProducerController;

namespace Aaron.Akka.ReliableDelivery.Cluster.Sharding.Internal;

internal sealed class ShardingProducerController<T>: ReceiveActor, IWithStash, IWithTimers
{
    public string ProducerId { get; }
    
    public IActorRef ShardRegion { get; }
    
    public ShardingProducerController.Settings Settings { get; }
    
    public Option<IActorRef> DurableQueueRef { get; private set; } = Option<IActorRef>.None;
    private readonly Option<Props> _durableQueueProps;
    
    private readonly ILoggingAdapter _log = Context.GetLogger();
    public IStash Stash { get; set; } = null!;
    
    public ITimerScheduler Timers { get; set; } = null!;

    public ShardingProducerController(string producerId, IActorRef shardRegion, Option<Props> durableQueueProps, ShardingProducerController.Settings settings)
    {
        ProducerId = producerId;
        ShardRegion = shardRegion;
        _durableQueueProps = durableQueueProps;
        Settings = settings;
        
        WaitingForStart();
    }

    protected override void PreStart()
    {
        DurableQueueRef = AskLoadState();
    }

    #region Behaviors

    private void WaitingForStart()
    {
        
    }

    private void BecomeActive()
    {
        
    }

    private void Active()
    {
        
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