using System;
using System.Linq;
using Aaron.Akka.ReliableDelivery.Internal;
using Akka.IO;
using FluentAssertions;
using Xunit;
using static Aaron.Akka.ReliableDelivery.DurableProducerQueue;

namespace Aaron.Akka.ReliableDelivery.Tests
{
    public class DurableProducerQueueSpecs
    {
        [Fact]
        public void DurableProducerQueueState_must_addMessageSent()
        {
            var state1 = State<string>.Empty.AddMessageSent(new MessageSent<string>(1, "a", false, "", 0));
            state1.Unconfirmed.Count.Should().Be(1);
            state1.Unconfirmed.First().Message.Equals("a").Should().BeTrue();
            state1.CurrentSeqNo.Should().Be(2);

            var state2 = state1.AddMessageSent(new MessageSent<string>(2, "b", false, "", 0));
            state2.Unconfirmed.Count.Should().Be(2);
            state2.Unconfirmed.Last().Message.Equals("b").Should().BeTrue();
            state2.CurrentSeqNo.Should().Be(3);
        }

        [Fact]
        public void DurableProducerQueueState_must_confirm()
        {
            var state1 = State<string>.Empty.AddMessageSent(new MessageSent<string>(1, "a", false, "", 0))
                .AddMessageSent(new MessageSent<string>(2, "b", false, "", 0));
            var state2 = state1.AddConfirmed(1L, "", 0);
            state2.Unconfirmed.Count.Should().Be(1);
            state2.Unconfirmed.First().Message.Equals("b").Should().BeTrue();
            state2.CurrentSeqNo.Should().Be(3);
        }

        [Fact]
        public void DurableProducerQueueState_must_filterPartiallStoredChunkedMessages()
        {
            var state1 = State<string>.Empty.AddMessageSent(MessageSent<string>.FromChunked(1,
                    new ChunkedMessage(ByteString.FromString("a"), true, true, 20, ""), false, "", 0))
                .AddMessageSent(MessageSent<string>.FromChunked(2,
                    new ChunkedMessage(ByteString.FromString("b"), true, false, 20, ""), false, "", 0))
                .AddMessageSent(MessageSent<string>.FromChunked(3,
                    new ChunkedMessage(ByteString.FromString("c"), false, false, 20, ""), false, "", 0));
            // last chunk was never stored

            var state2 = state1.CleanUpPartialChunkedMessages();
            state2.Unconfirmed.Count.Should().Be(1);
            state2.Unconfirmed.First().Message.Chunk!.Value.SerializedMessage.Should()
                .BeEquivalentTo(ByteString.FromString("a"));
            state2.CurrentSeqNo.Should().Be(2);
        }
    }
}