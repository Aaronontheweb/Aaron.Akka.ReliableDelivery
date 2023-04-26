﻿// -----------------------------------------------------------------------
//  <copyright file="TestDurableQueue.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2023 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using Akka.Actor;
using Akka.Event;
using static Aaron.Akka.ReliableDelivery.DurableProducerQueue;

namespace Aaron.Akka.ReliableDelivery.Tests;

public static class TestDurableProducerQueue
{
    /// <summary>
    /// Used to simplify tests.
    /// </summary>
    public const long TestTimestamp = long.MaxValue;

    public static Props CreateProps<T>(TimeSpan delay, State<T> initialState,
        Predicate<IDurableProducerQueueCommand<T>> failWhen)
    {
        return Props.Create(() => new TestDurableProducerQueue<T>(delay, failWhen, initialState));
    }

    public static Props CreateProps<T>(TimeSpan delay, State<T> initialState)
    {
        return Props.Create(() => new TestDurableProducerQueue<T>(delay, _ => false, initialState));
    }
}

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

    public TestDurableProducerQueue(TimeSpan delay, Predicate<IDurableProducerQueueCommand<T>> failWhen,
        State<T> initialState)
    {
        _delay = delay;
        _failWhen = failWhen;
        CurrentState = initialState;
        Active();
    }

    private void Active()
    {
        Receive<LoadState<T>>(cmd =>
        {
            MaybeFail(cmd);
            if (_delay == TimeSpan.Zero)
                cmd.ReplyTo.Tell(CurrentState);
            else
                Context.System.Scheduler.ScheduleTellOnce(_delay, cmd.ReplyTo, CurrentState, Self);
        });

        Receive<StoreMessageSent<T>>(cmd =>
        {
            if (cmd.MessageSent.SeqNr == CurrentState.CurrentSeqNr)
            {
                _log.Info("StoreMessageSent  seqNr {0}, confirmationQualifier [{1}]", cmd.MessageSent.SeqNr,
                    cmd.MessageSent.ConfirmationQualifier);
                MaybeFail(cmd);
                var reply = new StoreMessageSentAck(cmd.MessageSent.SeqNr);
                if (_delay == TimeSpan.Zero)
                    cmd.ReplyTo.Tell(reply);
                else
                    Context.System.Scheduler.ScheduleTellOnce(_delay, cmd.ReplyTo, reply, Self);
                CurrentState =
                    CurrentState.AddMessageSent(cmd.MessageSent.WithTimestamp(TestDurableProducerQueue.TestTimestamp));
            }
            else if (cmd.MessageSent.SeqNr == CurrentState.CurrentSeqNr - 1)
            {
                // already stored, could be a retry after timeout
                _log.Info("Duplicate seqNr {0}, currentSeqNr [{1}]", cmd.MessageSent.SeqNr, CurrentState.CurrentSeqNr);
                var reply = new StoreMessageSentAck(cmd.MessageSent.SeqNr);
                if (_delay == TimeSpan.Zero)
                    cmd.ReplyTo.Tell(reply);
                else
                    Context.System.Scheduler.ScheduleTellOnce(_delay, cmd.ReplyTo, reply, Self);
            }
            else
            {
                // may happen after failure
                _log.Info("Ignoring unexpected seqNr {0}, currentSeqNr [{1}]", cmd.MessageSent.SeqNr,
                    CurrentState.CurrentSeqNr);
                Unhandled(cmd);
            }
        });

        Receive<StoreMessageConfirmed<T>>(cmd =>
        {
            _log.Info("StoreMessageConfirmed seqNr [{0}], confirmationQualifier [{1}]", cmd.SeqNr,
                cmd.ConfirmationQualifier);
            MaybeFail(cmd);
            CurrentState = CurrentState.AddConfirmed(cmd.SeqNr, cmd.ConfirmationQualifier,
                TestDurableProducerQueue.TestTimestamp);
        });
    }

    private void MaybeFail(IDurableProducerQueueCommand<T> cmd)
    {
        if (_failWhen(cmd))
            throw new Exception($"TestDurableProducerQueue failed at {cmd}");
    }

    protected override void PreStart()
    {
        CurrentState = CurrentState.CleanUpPartialChunkedMessages();
        _log.Info("Starting with seqNr [{0}], confirmedSeqNr [{1}]", CurrentState.CurrentSeqNr,
            string.Join(",",
                CurrentState.ConfirmedSeqNr.Select(c => $"[{c.Key}] -> (low {c.Value.Item1}, high {c.Value.Item2})")));
    }
}