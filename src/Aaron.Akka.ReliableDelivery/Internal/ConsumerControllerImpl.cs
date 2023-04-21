using System;
using System.Buffers;
using System.Collections.Immutable;
using System.Linq;
using Akka.Actor;
using Akka.Event;
using Akka.IO;
using Akka.Serialization;
using Akka.Util;
using Akka.Util.Extensions;
using Akka.Util.Internal;
using static Aaron.Akka.ReliableDelivery.ConsumerController;
using static Aaron.Akka.ReliableDelivery.ProducerController;

namespace Aaron.Akka.ReliableDelivery.Internal;

internal sealed class ConsumerController<T> : ReceiveActor, IWithTimers, IWithStash
{
    private readonly ILoggingAdapter _log = Context.GetLogger();

    private Option<IActorRef> _producerControllerRegistration;
    public ConsumerController.Settings Settings { get; }
    public State CurrentState { get; private set; }
    private RetryTimer _retryTimer;
    private Serialization _serialization = Context.System.Serialization;
    public bool ResendLost => !Settings.OnlyFlowControl;

    public ConsumerController(Option<IActorRef> producerControllerRegistration, ConsumerController.Settings settings)
    {
        _producerControllerRegistration = producerControllerRegistration;
        Settings = settings;
        _retryTimer = new RetryTimer(Settings.ResendIntervalMin, Settings.ResendIntervalMax, Timers);

        WaitForStart();
    }

    private void WaitForStart()
    {
        var stopping = false;

        Receive<RegisterToProducerController<T>>(reg =>
        {
            reg.ProducerController.Tell(new RegisterConsumer<T>(Self));
            _producerControllerRegistration = reg.ProducerController.AsOption();
        });

        Receive<ConsumerController.Start<T>>(start =>
        {
            AssertLocalConsumer(start.DeliverTo);
            Context.WatchWith(start.DeliverTo, new ConsumerTerminated(start.DeliverTo));

            _log.Debug("Received Start, unstashing messages");
            CurrentState = InitialState(start, _producerControllerRegistration, stopping);
            Stash.UnstashAll();
            Become(Active);
        });

        Receive<SequencedMessage<T>>(seqMsg =>
        {
            // TODO: need a way of check if stash is at capacity
            _log.Debug("Received SequencedMessage seqNr [{0}], stashing before Start.", seqMsg.SeqNr);
            Stash.Stash();
        });

        Receive<DeliverThenStop<T>>(_ =>
        {
            // TODO: check if Stash is empty
            stopping = true;
        });

        Receive<Retry>(_ =>
        {
            if (_producerControllerRegistration.HasValue)
            {
                _log.Debug("Retry sending RegisterConsumer to [{0}]", _producerControllerRegistration);
                _producerControllerRegistration.Value.Tell(new RegisterConsumer<T>(Self));
            }
        });

        Receive<ConsumerTerminated>(terminated =>
        {
            _log.Debug("Consumer [{0}] terminated.", terminated.Consumer);
            Context.Stop(Self);
        });
    }

    private void Active()
    {
        Receive<SequencedMessage<T>>(seqMsg =>
        {
            var pid = seqMsg.ProducerId;
            var seqNr = seqMsg.SeqNr;
            var expectedSeqNr = CurrentState.ReceivedSeqNr + 1;

            _retryTimer.Reset();

            if (CurrentState.IsProducerChanged(seqMsg))
            {
                if (seqMsg.First && _log.IsDebugEnabled)
                    _log.Debug("Received first SequencedMessage from seqNr [{0}], delivering to consumer.", seqNr);
            }
        });
    }

    private void WaitingForConfirmation(SequencedMessage<T> sequencedMessage)
    {
        Receive<Confirmed>(c =>
        {
            var seqNr = sequencedMessage.SeqNr;
            if (_log.IsDebugEnabled)
            {
                _log.Debug("Received Confirmed seqNr [{0}] from consumer.", seqNr);
            }

            long ComputeNextSeqNr()
            {
                if (sequencedMessage.First)
                {
                    // confirm the first message immediately to cancel resending of first
                    var newRequestedSeqNr = seqNr - 1 + Settings.FlowControlWindow;
                    _log.Debug("Sending Request after first with confirmedSeqNr [{0}], requestUpToSeqNr [{1}]", seqNr, newRequestedSeqNr);
                    CurrentState.ProducerController.Tell(new Request(seqNr, newRequestedSeqNr, ResendLost, false));
                    return newRequestedSeqNr;
                } else if ((CurrentState.RequestedSeqNr - seqNr) == Settings.FlowControlWindow / 2)
                {
                    var newRequestedSeqNr = CurrentState.RequestedSeqNr + Settings.FlowControlWindow / 2;
                    _log.Debug("Sending Request with confirmedSeqNr [{0}], requestUpToSeqNr [{1}]", seqNr, newRequestedSeqNr);
                    CurrentState.ProducerController.Tell(new Request(seqNr, newRequestedSeqNr, ResendLost, false));
                    _retryTimer.Start(); // reset interval since Request was just sent
                    return newRequestedSeqNr;
                }
                else
                {
                    if (sequencedMessage.Ack)
                    {
                        if (_log.IsDebugEnabled)
                            _log.Debug("Sending Ack seqNr [{0}]", seqNr);
                        CurrentState.ProducerController.Tell(new Ack(seqNr));
                    }

                    return CurrentState.RequestedSeqNr;
                }
            }

            var requestedSeqNr = ComputeNextSeqNr();
            if (CurrentState.Stopping) // TODO: need to be able to check stash size in future version of Akka.NET
            {
                //if (Stash.IsEmpty)
                {
                    _log.Debug("Stopped at seqNr [{0}], after delivery of buffered messages.", seqNr);
                    // best effort to Ack latest confirmed when stopping
                    CurrentState.ProducerController.Tell(new Ack(seqNr));
                    Context.Stop(Self);
                }
            }
            else
            {
                CurrentState = CurrentState.Copy(confirmedSeqNr: seqNr, requestedSeqNr: requestedSeqNr);
                Stash.Unstash();
                Become(Active);
            }
        });

        Receive<SequencedMessage<T>>(nextSeq =>
        {
            var expectedSeqNr = sequencedMessage.SeqNr + 1; // need to compute stash buffer size
        });
    }

    #region Internal Methods

    private void ReceiveChangedProducer(SequencedMessage<T> seqMsg)
    {
        var seqNr = seqMsg.SeqNr;

        if (seqMsg.First || !ResendLost)
        {
            LogChangedProducer(seqMsg);
            var newRequestedSeqNr = seqMsg.SeqNr - 1 + Settings.FlowControlWindow;
            _log.Debug("Sending Request with requestUpToSeqNr [{0}] after first SequencedMessage.", newRequestedSeqNr);
            seqMsg.ProducerController.Tell(new Request(0, newRequestedSeqNr, ResendLost, false));
        }
    }

    private void LogChangedProducer(SequencedMessage<T> seqMsg)
    {
        if (CurrentState.ProducerController == Context.System.DeadLetters)
        {
            _log.Debug("Associated with new ProducerController [{0}], seqNr [{1}].", seqMsg.ProducerController,
                seqMsg.SeqNr);
        }
        else
        {
            _log.Debug("Changing ProducerController from [{0}] to [{1}], seqNr [{2}]", CurrentState.ProducerController,
                seqMsg.ProducerController, seqMsg.SeqNr);
        }
    }

    private void Deliver(SequencedMessage<T> seqMsg)
    {
        var previouslyCollectedChunks =
            seqMsg.IsFirstChunk ? ImmutableList<SequencedMessage<T>>.Empty : CurrentState.CollectedChunks;
        if (seqMsg.IsLastChunk)
        {
            var assembledSeqMsg = !seqMsg.Message.IsMessage
                ? AssembleChunks(previouslyCollectedChunks.Add(seqMsg))
                : seqMsg;
            CurrentState.Consumer.Tell(new Delivery<T>(assembledSeqMsg.Message.Message!, Context.Self, seqMsg.ProducerId, seqMsg.SeqNr));
        }
    }

    private SequencedMessage<T> AssembleChunks(ImmutableList<SequencedMessage<T>> collectedChunks)
    {
        var reverseCollectedChunks = collectedChunks; // no need to actually reverse the list
        var bufferSize = reverseCollectedChunks.Sum(chunk => chunk.Message.Chunk!.Value.SerializedMessage.Count);
        byte[] bytes;
        using (var mem = MemoryPool<byte>.Shared.Rent(bufferSize))
        {
            var curIndex = 0;
            var memory = mem.Memory;
            foreach (var b in reverseCollectedChunks.Select(c => c.Message.Chunk!.Value.SerializedMessage))
            {
                b.CopyTo(ref memory, curIndex, b.Count);
                curIndex += b.Count;
            }

            bytes = memory.ToArray();
        }

        var headMessage = reverseCollectedChunks.First(); // this is the last chunk
        var headChunk = headMessage.Message.Chunk!.Value;
        var message = (T)_serialization.Deserialize(bytes, headChunk.SerializerId, headChunk.Manifest);
        return new SequencedMessage<T>(headMessage.ProducerId, headMessage.SeqNr, message, headMessage.First, headMessage.Ack, headMessage.ProducerController);
    }

    private static State InitialState(ConsumerController.Start<T> start, Option<IActorRef> registering, bool stopping)
    {
        return new State(Context.System.DeadLetters, "n/a", start.DeliverTo, 0, 0, 0,
            ImmutableList<SequencedMessage<T>>.Empty, registering, stopping);
    }

    #endregion

    #region Internal Types

    private sealed class RetryTimer
    {
        public RetryTimer(TimeSpan minBackoff, TimeSpan maxBackoff, ITimerScheduler timers)
        {
            MinBackoff = minBackoff;
            MaxBackoff = maxBackoff;
            Interval = minBackoff;
            Timers = timers;
        }

        public ITimerScheduler Timers { get; }

        public TimeSpan MinBackoff { get; }

        public TimeSpan MaxBackoff { get; }

        public TimeSpan Interval { get; private set; }

        public void Start()
        {
            Interval = MinBackoff;
            // todo: when we have timers with fixed delays, call it here
            Timers.StartSingleTimer(Retry.Instance, Retry.Instance, Interval);
        }

        public void ScheduleNext()
        {
            var newInterval = Interval == MaxBackoff
                ? MaxBackoff
                : MaxBackoff.Min(TimeSpan.FromSeconds(Interval.TotalSeconds * 1.5));
            if (newInterval != Interval)
            {
                Timers.StartPeriodicTimer(Retry.Instance, Retry.Instance, newInterval);
            }
        }

        public void Reset()
        {
            if (Interval != MinBackoff)
                Start();
        }
    }

    private sealed class Retry
    {
        private Retry()
        {
        }

        public static readonly Retry Instance = new();
    }

    private sealed class ConsumerTerminated
    {
        public ConsumerTerminated(IActorRef consumer)
        {
            Consumer = consumer;
        }

        public IActorRef Consumer { get; }
    }

    internal readonly struct State
    {
        public State(
            IActorRef producerController,
            string producerId,
            IActorRef consumer,
            long receivedSeqNr,
            long confirmedSeqNr,
            long requestedSeqNr,
            ImmutableList<SequencedMessage<T>> collectedChunks,
            Option<IActorRef> registering,
            bool stopping)
        {
            ProducerController = producerController;
            ProducerId = producerId;
            Consumer = consumer;
            ReceivedSeqNr = receivedSeqNr;
            ConfirmedSeqNr = confirmedSeqNr;
            RequestedSeqNr = requestedSeqNr;
            CollectedChunks = collectedChunks;
            Registering = registering;
            Stopping = stopping;
        }

        public IActorRef ProducerController { get; }

        public string ProducerId { get; }

        public IActorRef Consumer { get; }

        public long ReceivedSeqNr { get; }

        public long ConfirmedSeqNr { get; }

        public long RequestedSeqNr { get; }

        public ImmutableList<SequencedMessage<T>> CollectedChunks { get; }

        public Option<IActorRef> Registering { get; }

        public bool Stopping { get; }

        public bool IsNextExpected(SequencedMessage<T> seqMsg)
        {
            return seqMsg.SeqNr == ReceivedSeqNr + 1;
        }

        public bool IsProducerChanged(SequencedMessage<T> seqMsg)
        {
            return !seqMsg.ProducerController.Equals(ProducerController) || ReceivedSeqNr == 0;
        }

        public State ClearCollectedChunks()
        {
            return CollectedChunks.IsEmpty ? this : Copy(collectedChunks: ImmutableList<SequencedMessage<T>>.Empty);
        }

        // add a copy method that can optionally overload every property
        public State Copy(
            IActorRef? producerController = null,
            string? producerId = null,
            IActorRef? consumer = null,
            long? receivedSeqNr = null,
            long? confirmedSeqNr = null,
            long? requestedSeqNr = null,
            ImmutableList<SequencedMessage<T>>? collectedChunks = null,
            Option<IActorRef>? registering = null,
            bool? stopping = null)
        {
            return new State(
                producerController ?? ProducerController,
                producerId ?? ProducerId,
                consumer ?? Consumer,
                receivedSeqNr ?? ReceivedSeqNr,
                confirmedSeqNr ?? ConfirmedSeqNr,
                requestedSeqNr ?? RequestedSeqNr,
                collectedChunks ?? CollectedChunks,
                registering ?? Registering,
                stopping ?? Stopping);
        }
    }

    #endregion

    public ITimerScheduler Timers { get; set; } = null!;
    public IStash Stash { get; set; } = null!;
}