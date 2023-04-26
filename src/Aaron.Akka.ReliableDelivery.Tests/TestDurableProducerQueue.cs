// -----------------------------------------------------------------------
//  <copyright file="TestDurableQueue.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2023 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Actor;
using Akka.Event;
using static Aaron.Akka.ReliableDelivery.DurableProducerQueue;

namespace Aaron.Akka.ReliableDelivery.Tests;

/// <summary>
/// INTERNAL API
/// </summary>
/// <typeparam name="T">The type of messages handled by the durable queue.</typeparam>
public class TestDurableProducerQueue<T> : ReceiveActor
{
    private readonly ILoggingAdapter _log = Context.GetLogger();
    private readonly TimeSpan _delay;
    private readonly Predicate<IDurableProducerQueueCommand<T>> _failWhen;
    
    public State<T> CurrentState { get; private set; }

    public TestDurableProducerQueue(TimeSpan delay, Predicate<IDurableProducerQueueCommand<T>> failWhen, State<T> initialState)
    {
        _delay = delay;
        _failWhen = failWhen;
        CurrentState = initialState;
    }

    protected override void PreStart()
    {
        _log.Info("Starting with seqNr [{0}], confirmedSeqNr [{1}]", CurrentState.CurrentSeqNr, CurrentState.ConfirmedSeqNr);
    }
}