// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using MessagePack;
using Microsoft.AspNetCore.SignalR.Protocol;
using System.Collections.Generic;

namespace Microsoft.AspNetCore.SignalR
{
    public class MessagePackHubProtocolOptions
    {
        public IList<IFormatterResolver> FormatterResolvers { get; set; } = MessagePackHubProtocol.CreateDefaultFormatterResolvers();
    }
}