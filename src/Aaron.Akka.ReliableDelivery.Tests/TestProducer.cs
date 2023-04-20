// -----------------------------------------------------------------------
//  <copyright file="TestProducer.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2023 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Actor;

namespace Aaron.Akka.ReliableDelivery.Tests;

public class TestConsumer
{
    public sealed class Job
    {
        public Job(string payload)
        {
            Payload = payload;
        }

        public string Payload { get; }
    }

    public interface ICommand
    {
    }
    
    public sealed class JobDelivery : ICommand
    {
        public JobDelivery(Job msg, IActorRef confirmTo, string producerId, long seqNr)
        {
            Msg = msg;
            ConfirmTo = confirmTo;
            ProducerId = producerId;
            SeqNr = seqNr;
        }

        public Job Msg { get; }
        public IActorRef ConfirmTo { get; }
        public string ProducerId { get; }
        public long SeqNr { get; }
    }
    
    public sealed class SomeAsyncJob : ICommand
    {
        public SomeAsyncJob(Job msg, IActorRef confirmTo, string producerId, long seqNr)
        {
            Msg = msg;
            ConfirmTo = confirmTo;
            ProducerId = producerId;
            SeqNr = seqNr;
        }

        public Job Msg { get; }
        public IActorRef ConfirmTo { get; }
        public string ProducerId { get; }
        public long SeqNr { get; }
    }
}

public class TestProducer
{
    public interface ICommand{ }
    public sealed class Tick : ICommand
    {
        public static readonly Tick Instance = new Tick();
        private Tick() { }
    }
    
    public TestProducer(TimeSpan delay)
    {
        Delay = delay;
    }

    public int CurrentSequenceNr { get; private set; }
    public TimeSpan Delay { get; }
}