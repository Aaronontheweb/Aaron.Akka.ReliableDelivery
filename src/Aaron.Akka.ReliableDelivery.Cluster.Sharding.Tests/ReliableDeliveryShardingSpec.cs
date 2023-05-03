using Aaron.Akka.ReliableDelivery.Cluster.Sharding;
using Aaron.Akka.ReliableDelivery.Cluster.Sharding.Internal;
using Aaron.Akka.ReliableDelivery.Internal;
using Aaron.Akka.ReliableDelivery.Tests;
using Akka.Actor;
using Akka.Actor.Dsl;
using Akka.Cluster;
using Akka.Cluster.Sharding;
using Akka.Configuration;
using Akka.Event;
using Akka.TestKit.Xunit2;
using Akka.Util;
using FluentAssertions;
using Xunit.Abstractions;
using static Aaron.Akka.ReliableDelivery.Tests.TestConsumer;

namespace Sharding.Tests;

public class ReliableDeliveryShardingSpec : TestKit
{
    public static Config Configuration = @"
        akka.loglevel = DEBUG
        akka.actor.provider = cluster
        akka.remote.dot-netty.tcp.port = 0
        akka.reliable-delivery.consumer-controller.flow-control-window = 20
    ";

    public ReliableDeliveryShardingSpec(ITestOutputHelper output) : base(
        Configuration.WithFallback(RdConfig.DefaultConfig()), output: output)
    {
    }

    private class TestShardingProducer : ReceiveActor, IWithTimers
    {
        public sealed class Tick
        {
            public static Tick Instance { get; } = new();

            private Tick()
            {
            }
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
            _sendNextAdapter =
                Context.ActorOf(
                    act =>
                    {
                        act.Receive<ShardingProducerController.RequestNext<Job>>((next, _) =>
                            self.Tell(new RequestNext(next.SendNextTo)));
                        
                        act.OnPreStart = ctx => _producerController.Tell(new ShardingProducerController.Start<Job>(ctx.Self));
                    }, "sendNextAdapter");
        }

        private void Idle(int n)
        {
            Receive<Tick>(_ => { }); // ignore
            Receive<RequestNext>(next => { Become(() => Active(n + 1, next.SendNextTo)); });
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

    private int _idCount = 0;
    private int NextId() => _idCount++;

    private string ProducerId => $"p-{_idCount}";

    private async Task JoinCluster()
    {
        var cluster = Cluster.Get(Sys);
        await cluster.JoinAsync(cluster.SelfAddress);
        await AwaitAssertAsync(() => Assert.True(cluster.IsUp));
    }

    [Fact]
    public async Task ReliableDelivery_with_Sharding_must_illustrate_Sharding_usage()
    {
        await JoinCluster();
        NextId();

        var consumerEndProbe = CreateTestProbe();
        var region = await ClusterSharding.Get(Sys).StartAsync($"TestConsumer-{_idCount}", s =>
            ShardingConsumerController.Create<Job>(c =>
                    TestConsumer.PropsFor(DefaultConsumerDelay, 42, consumerEndProbe.Ref, c),
                ShardingConsumerController.Settings.Create(Sys)), ClusterShardingSettings.Create(Sys), HashCodeMessageExtractor.Create(10,
            o =>
            {
                if (o is ShardingEnvelope se)
                    return se.EntityId;
                return string.Empty;
            }, o =>
            {
                if (o is ShardingEnvelope se)
                    return se.Message;
                return o;
            }));

        var producerController =
            Sys.ActorOf(
                ShardingProducerController.Create<Job>(ProducerId, region, Option<Props>.None,
                    ShardingProducerController.Settings.Create(Sys)), $"shardingController-{_idCount}");
        var producer = Sys.ActorOf(Props.Create(() => new TestShardingProducer(producerController)),
            $"producer-{_idCount}");
        
        // expecting 3 end messages, one for each entity: "entity-0", "entity-1", "entity-2"
        consumerEndProbe.ReceiveN(3, TimeSpan.FromSeconds(5));
    }
    
    [Fact]
    public async Task ReliableDelivery_with_Sharding_must_illustrate_Sharding_usage_with_several_producers()
    {
        await JoinCluster();
        NextId();

        var consumerEndProbe = CreateTestProbe();
        var region = await ClusterSharding.Get(Sys).StartAsync($"TestConsumer-{_idCount}", s =>
            ShardingConsumerController.Create<Job>(c =>
                    TestConsumer.PropsFor(DefaultConsumerDelay, 42, consumerEndProbe.Ref, c),
                ShardingConsumerController.Settings.Create(Sys)), ClusterShardingSettings.Create(Sys), HashCodeMessageExtractor.Create(10,
            o =>
            {
                if (o is ShardingEnvelope se)
                    return se.EntityId;
                return string.Empty;
            }, o =>
            {
                if (o is ShardingEnvelope se)
                    return se.Message;
                return o;
            }));

        var shardingController1 =
            Sys.ActorOf(
                ShardingProducerController.Create<Job>($"p1-{_idCount}", region, Option<Props>.None,
                    ShardingProducerController.Settings.Create(Sys)), $"shardingController1-{_idCount}");
        var producer1 = Sys.ActorOf(Props.Create(() => new TestShardingProducer(shardingController1)),
            $"p1-{_idCount}");
        
        var shardingController2 =
            Sys.ActorOf(
                ShardingProducerController.Create<Job>($"p2-{_idCount}", region, Option<Props>.None,
                    ShardingProducerController.Settings.Create(Sys)), $"shardingController2-{_idCount}");
        var producer2 = Sys.ActorOf(Props.Create(() => new TestShardingProducer(shardingController2)),
            $"p2-{_idCount}");
        
        // expecting 3 end messages, one for each entity: "entity-0", "entity-1", "entity-2"
        var endMessages = consumerEndProbe.ReceiveN(3, TimeSpan.FromSeconds(5));

        var producerIds = endMessages.Cast<Collected>().SelectMany(c => c.ProducerIds).ToList();
            producerIds
            .Should().BeEquivalentTo(new[]
            {
                $"p1-{_idCount}-entity-0",
                $"p1-{_idCount}-entity-1",
                $"p1-{_idCount}-entity-2",
                $"p2-{_idCount}-entity-0",
                $"p2-{_idCount}-entity-1",
                $"p2-{_idCount}-entity-2"
            });
    }

    [Fact]
    public async Task ReliableDelivery_with_Sharding_must_reply_to_MessageWithConfirmation()
    {
        await JoinCluster();
        NextId();

        var consumerEndProbe = CreateTestProbe();
        var region = await ClusterSharding.Get(Sys).StartAsync($"TestConsumer-{_idCount}", s =>
            ShardingConsumerController.Create<Job>(c =>
                    TestConsumer.PropsFor(DefaultConsumerDelay, 3, consumerEndProbe.Ref, c),
                ShardingConsumerController.Settings.Create(Sys)), ClusterShardingSettings.Create(Sys), HashCodeMessageExtractor.Create(10,
            o =>
            {
                if (o is ShardingEnvelope se)
                    return se.EntityId;
                return string.Empty;
            }, o =>
            {
                if (o is ShardingEnvelope se)
                    return se.Message;
                return o;
            }));

        var producerController =
            Sys.ActorOf(
                ShardingProducerController.Create<Job>(ProducerId, region, Option<Props>.None,
                    ShardingProducerController.Settings.Create(Sys)), $"shardingController-{_idCount}");

        var producerProbe = CreateTestProbe();
        producerController.Tell(new ShardingProducerController.Start<Job>(producerProbe.Ref));

        var replyProbe = CreateTestProbe();
        (await producerProbe.ExpectMsgAsync<ShardingProducerController.RequestNext<Job>>())
            .AskNextTo(new ShardingProducerController.MessageWithConfirmation<Job>("entity-0", new Job("msg-1"),
                replyProbe.Ref));
        (await producerProbe.ExpectMsgAsync<ShardingProducerController.RequestNext<Job>>())
            .AskNextTo(new ShardingProducerController.MessageWithConfirmation<Job>("entity-0", new Job("msg-2"),
                replyProbe.Ref));
        (await producerProbe.ExpectMsgAsync<ShardingProducerController.RequestNext<Job>>())
            .AskNextTo(new ShardingProducerController.MessageWithConfirmation<Job>("entity-1", new Job("msg-3"),
                replyProbe.Ref));
        (await producerProbe.ExpectMsgAsync<ShardingProducerController.RequestNext<Job>>())
            .AskNextTo(new ShardingProducerController.MessageWithConfirmation<Job>("entity-0", new Job("msg-4"),
                replyProbe.Ref));

        await consumerEndProbe.ExpectMsgAsync<Collected>(); // entity-0 received 3 messages
        await consumerEndProbe.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(100));
        
        (await producerProbe.ExpectMsgAsync<ShardingProducerController.RequestNext<Job>>())
            .AskNextTo(new ShardingProducerController.MessageWithConfirmation<Job>("entity-1", new Job("msg-5"),
                replyProbe.Ref));
        (await producerProbe.ExpectMsgAsync<ShardingProducerController.RequestNext<Job>>())
            .AskNextTo(new ShardingProducerController.MessageWithConfirmation<Job>("entity-1", new Job("msg-6"),
                replyProbe.Ref));
        
        await consumerEndProbe.ExpectMsgAsync<Collected>(); // entity-1 received 3 messages
    }
}