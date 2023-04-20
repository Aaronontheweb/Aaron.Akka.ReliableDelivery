// -----------------------------------------------------------------------
//  <copyright file="TestProducer.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2023 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;

namespace Aaron.Akka.ReliableDelivery.Tests;

public class TestProducer
{
    public interface ICommand
    {
    }

    public sealed class Tick : ICommand
    {
        public static readonly Tick Instance = new Tick();

        private Tick()
        {
        }
    }

    public TestProducer(TimeSpan delay)
    {
        Delay = delay;
    }

    public int CurrentSequenceNr { get; private set; }
    public TimeSpan Delay { get; }
}