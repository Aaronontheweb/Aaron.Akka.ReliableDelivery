// -----------------------------------------------------------------------
//  <copyright file="TestProducer.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2023 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading.Channels;
using Akka.Actor;
using Akka.Event;

namespace Aaron.Akka.ReliableDelivery.Tests;

/// <summary>
/// INTERNAL API.
/// </summary>
public class TestProducer : ReceiveActor, IWithTimers
{
    public sealed class Tick
    {
        public static readonly Tick Instance = new Tick();

        private Tick()
        {
        }
    }
    
    public sealed class WriteNext
    {
        public static readonly WriteNext Instance = new WriteNext();

        private WriteNext()
        {
        }
    }
    
    public int CurrentSequenceNr { get; private set; }
    public TimeSpan Delay { get; }
    public ITimerScheduler Timers { get; set; } = null!;
    private readonly IActorRef _producerController;
    private readonly ILoggingAdapter _log = Context.GetLogger();

    public TestProducer(TimeSpan delay, IActorRef producerController)
    {
        Delay = delay;
        _producerController = producerController;
        WaitingForController();
    }

    protected override void PreStart()
    {
        _producerController.Tell(new ProducerController.Start<TestConsumer.Job>(Self));
    }

    private void WaitingForController()
    {
        Receive<ProducerController.StartProduction<TestConsumer.Job>>(production =>
        {
            _log.Info("Received StartProduction signal from producer [{0}]", production.ProducerId);
            if(Delay == TimeSpan.Zero)
                Become(() => ActiveNoDelay(production.Writer));
            else
            {
                Timers.StartPeriodicTimer("tick", Tick.Instance, Delay);
                Become(() => Active(production.Writer));
            }
        });
    }

    private void ActiveNoDelay(ChannelWriter<ProducerController.SendNext<TestConsumer.Job>> writer)
    {
        // no ticks in this behavior
        ReceiveAsync<WriteNext>(async _ =>
        {
            await writer.WriteAsync(CreateMessage(CurrentSequenceNr));
            Self.Tell(WriteNext.Instance);
            CurrentSequenceNr++;
        });
    }
    
    private void Active(ChannelWriter<ProducerController.SendNext<TestConsumer.Job>> writer)
    {
        Receive<Tick>(_ =>
        {
            writer.WriteAsync(CreateMessage(CurrentSequenceNr)).PipeTo(Self, success: () => WriteNext.Instance);
            BecomeStacked(Idle);
        });

        Receive<WriteNext>(_ =>
        {
            throw new InvalidOperationException("Unexpected WriteNext message, already have one.");
        });
    }
    
    private void Idle()
    {
        Receive<Tick>(_ => { }); // ignore
        Receive<WriteNext>(_ =>
        {
            CurrentSequenceNr++;
            UnbecomeStacked();
        });
    }

    private ProducerController.SendNext<TestConsumer.Job> CreateMessage(int n)
    {
        var msg = $"msg-{n}";
        _log.Info("Sent [{0}]", n);
        return new ProducerController.SendNext<TestConsumer.Job>(new TestConsumer.Job(msg), ActorRefs.NoSender);
    }
}