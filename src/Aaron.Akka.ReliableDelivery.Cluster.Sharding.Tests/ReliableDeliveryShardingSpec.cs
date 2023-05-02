using Aaron.Akka.ReliableDelivery.Cluster.Sharding.Internal;
using Aaron.Akka.ReliableDelivery.Tests;
using Akka.Actor;
using Akka.Actor.Dsl;
using Akka.Cluster.Sharding;
using Akka.Configuration;
using Akka.Event;
using Akka.TestKit.Xunit2;
using Xunit.Abstractions;
using static Aaron.Akka.ReliableDelivery.Tests.TestConsumer;

namespace Aaron.Akka.ReliableDelivery.Cluster.Sharding.Tests;

public class ReliableDeliveryShardingSpec : TestKit
{
    public static Config Configuration = @"
        akka.actor.provider = cluster
        akka.remote.dot-netty.tcp.port = 0
        akka.reliable-delivery.consumer-controller.flow-control-window = 20
    ";
    
    public ReliableDeliveryShardingSpec(ITestOutputHelper output) : base(Configuration.WithFallback(RdShardingConfig.DefaultConfig()), output:output)
    {
    }

    private class TestShardingProducer : ReceiveActor, IWithTimers
    {
        public sealed class Tick
        {
            public static Tick Instance { get; } = new();
            private Tick() { }
        }
        
        public sealed record RequestNext(IActorRef SendNextTo);

        private readonly ILoggingAdapter _log = Context.GetLogger();

        public TestShardingProducer(IActorRef producerController)
        {
            _producerController = producerController;
            
            // simulate fast producer
            Timers.StartPeriodicTimer("tick", Tick.Instance, TimeSpan.FromMilliseconds(20));
            Idle(0);
        }

        public ITimerScheduler Timers { get; set; } = null!;
        private IActorRef _sendNextAdapter;
        private readonly IActorRef _producerController;
        
        protected override void PreStart()
        {
            var self = Self;
            _sendNextAdapter = Context.ActorOf(act =>
            {
                act.Receive<ShardingProducerController.RequestNext<Job>>((next, _) => self.Tell(new RequestNext(next.SendNextTo)));
            }, "sendNextAdapter");
        }

        private void Idle(int n)
        {
            Receive<Tick>(_ => { }); // ignore
            Receive<RequestNext>(next =>
            {
                Become(() => Active(n + 1, next.SendNextTo));
            });
        }

        private void Active(int n, IActorRef sendTo)
        {
            Receive<Tick>(_ =>
            {
                var msg = $"msg-{n}";
                var entityId = $"entity-{n % 3}";
                _log.Info("Sending {0} to {1}", msg, entityId);
                sendTo.Tell(new ShardingEnvelope(entityId, new Job(msg)));
                Become(() => Idle(n));
            });

            Receive<RequestNext>(next => { }); // already active
        }
    }
    
    [Fact]
    public void Test1()
    {
    }
}