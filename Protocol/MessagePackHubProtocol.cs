// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using MessagePack;
using MessagePack.Formatters;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Internal;
using Microsoft.Extensions.Options;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;

namespace Microsoft.AspNetCore.SignalR.Protocol
{
    /// <summary>
    /// Implements the SignalR Hub Protocol using MessagePack.
    /// </summary>
    public class MessagePackHubProtocol : IHubProtocol
    {
        private const int ErrorResult = 1;
        private const int VoidResult = 2;
        private const int NonVoidResult = 3;

        private MessagePackSerializerOptions _options;

        private static readonly string ProtocolName = "messagepack";
        private static readonly int ProtocolVersion = 1;
        private static readonly int ProtocolMinorVersion = 0;

        /// <inheritdoc />
        public string Name => ProtocolName;

        /// <inheritdoc />
        public int Version => ProtocolVersion;

        /// <inheritdoc />
        public int MinorVersion => ProtocolMinorVersion;

        /// <inheritdoc />
        public TransferFormat TransferFormat => TransferFormat.Binary;

        /// <summary>
        /// Initializes a new instance of the <see cref="MessagePackHubProtocol"/> class.
        /// </summary>
        public MessagePackHubProtocol()
            : this(Options.Create(new MessagePackHubProtocolOptions()))
        { }

        /// <summary>
        /// Initializes a new instance of the <see cref="MessagePackHubProtocol"/> class.
        /// </summary>
        /// <param name="options">The options used to initialize the protocol.</param>
        public MessagePackHubProtocol(IOptions<MessagePackHubProtocolOptions> options)
        {
            var msgPackOptions = options.Value;
            SetupResolver(msgPackOptions);
        }

        private void SetupResolver(MessagePackHubProtocolOptions options)
        {
            _options = MessagePackSerializerOptions.Standard;
            // if counts don't match then we know users customized resolvers so we set up the options
            // with the provided resolvers
            if (options.FormatterResolvers.Count != SignalRResolver.Resolvers.Count)
            {
                _options = _options.WithResolver(new CombinedResolvers(options.FormatterResolvers));
                return;
            }

            for (var i = 0; i < options.FormatterResolvers.Count; i++)
            {
                // check if the user customized the resolvers
                if (options.FormatterResolvers[i] != SignalRResolver.Resolvers[i])
                {
                    _options = _options.WithResolver(new CombinedResolvers(options.FormatterResolvers));
                    return;
                }
            }

            // Use optimized cached resolver if the default is chosen
            _options.WithResolver(SignalRResolver.Instance);
        }

        /// <inheritdoc />
        public bool IsVersionSupported(int version)
        {
            return version == Version;
        }

        /// <inheritdoc />
        public bool TryParseMessage(ref ReadOnlySequence<byte> input, IInvocationBinder binder, out HubMessage message)
        {
            if (!BinaryMessageParser.TryParseMessage(ref input, out var payload))
            {
                message = null;
                return false;
            }

            //var arraySegment = GetArraySegment(payload);
            MessagePackReader reader = new MessagePackReader(payload);

            message = ParseMessage(ref reader, binder, _options);
            return true;
        }

        private static ArraySegment<byte> GetArraySegment(in ReadOnlySequence<byte> input)
        {
            if (input.IsSingleSegment)
            {
                var isArray = MemoryMarshal.TryGetArray(input.First, out var arraySegment);
                // This will never be false unless we started using un-managed buffers
                Debug.Assert(isArray);
                return arraySegment;
            }

            // Should be rare
            return new ArraySegment<byte>(input.ToArray());
        }

        private static HubMessage ParseMessage(ref MessagePackReader reader, IInvocationBinder binder, MessagePackSerializerOptions options)
        {
            var readSize = reader.ReadArrayHeader();

            var messageType = reader.ReadInt32();

            switch (messageType)
            {
                case HubProtocolConstants.InvocationMessageType:
                    return CreateInvocationMessage(ref reader, binder, options);

                case HubProtocolConstants.StreamInvocationMessageType:
                    return CreateStreamInvocationMessage(ref reader, binder, options);

                case HubProtocolConstants.StreamItemMessageType:
                    return CreateStreamItemMessage(ref reader, binder, options);

                case HubProtocolConstants.CompletionMessageType:
                    return CreateCompletionMessage(ref reader, binder, options);

                case HubProtocolConstants.CancelInvocationMessageType:
                    return CreateCancelInvocationMessage(ref reader);

                case HubProtocolConstants.PingMessageType:
                    return PingMessage.Instance;

                case HubProtocolConstants.CloseMessageType:
                    return CreateCloseMessage(ref reader);
                //case HubProtocolConstants.StreamCompleteMessageType:
                //    return CreateStreamCompleteMessage(ref reader, ref startOffset);
                default:
                    // Future protocol changes can add message types, old clients can ignore them
                    return null;
            }
        }

        private static HubMessage CreateInvocationMessage(ref MessagePackReader reader, IInvocationBinder binder, MessagePackSerializerOptions options)
        {
            var headers = ReadHeaders(ref reader);
            var invocationId = ReadInvocationId(ref reader);

            // For MsgPack, we represent an empty invocation ID as an empty string,
            // so we need to normalize that to "null", which is what indicates a non-blocking invocation.
            if (string.IsNullOrEmpty(invocationId))
            {
                invocationId = null;
            }

            var target = ReadString(ref reader, "target");

            try
            {
                var parameterTypes = binder.GetParameterTypes(target);
                var arguments = BindArguments(ref reader, parameterTypes, options);
                return ApplyHeaders(headers, new InvocationMessage(invocationId, target, arguments));
            }
            catch (Exception ex)
            {
                return new InvocationBindingFailureMessage(invocationId, target, ExceptionDispatchInfo.Capture(ex));
            }
        }

        private static HubMessage CreateStreamInvocationMessage(ref MessagePackReader reader, IInvocationBinder binder, MessagePackSerializerOptions resolver)
        {
            var headers = ReadHeaders(ref reader);
            var invocationId = ReadInvocationId(ref reader);
            var target = ReadString(ref reader, "target");

            try
            {
                var parameterTypes = binder.GetParameterTypes(target);
                var arguments = BindArguments(ref reader, parameterTypes, resolver);
                return ApplyHeaders(headers, new StreamInvocationMessage(invocationId, target, arguments));
            }
            catch (Exception ex)
            {
                return new InvocationBindingFailureMessage(invocationId, target, ExceptionDispatchInfo.Capture(ex));
            }
        }

        private static StreamItemMessage CreateStreamItemMessage(ref MessagePackReader reader, IInvocationBinder binder, MessagePackSerializerOptions resolver)
        {
            var headers = ReadHeaders(ref reader);
            var invocationId = ReadInvocationId(ref reader);
            var itemType = binder.GetStreamItemType(invocationId);
            var value = DeserializeObject(ref reader, itemType, "item", resolver);
            return ApplyHeaders(headers, new StreamItemMessage(invocationId, value));
        }

        private static CompletionMessage CreateCompletionMessage(ref MessagePackReader reader, IInvocationBinder binder, MessagePackSerializerOptions resolver)
        {
            var headers = ReadHeaders(ref reader);
            var invocationId = ReadInvocationId(ref reader);
            var resultKind = ReadInt32(ref reader, "resultKind");

            string error = null;
            object result = null;
            var hasResult = false;

            switch (resultKind)
            {
                case ErrorResult:
                    error = ReadString(ref reader, "error");
                    break;

                case NonVoidResult:
                    var itemType = binder.GetReturnType(invocationId);
                    result = DeserializeObject(ref reader, itemType, "argument", resolver);
                    hasResult = true;
                    break;

                case VoidResult:
                    hasResult = false;
                    break;

                default:
                    throw new InvalidDataException("Invalid invocation result kind.");
            }

            return ApplyHeaders(headers, new CompletionMessage(invocationId, error, result, hasResult));
        }

        private static CancelInvocationMessage CreateCancelInvocationMessage(ref MessagePackReader reader)
        {
            var headers = ReadHeaders(ref reader);
            var invocationId = ReadInvocationId(ref reader);
            return ApplyHeaders(headers, new CancelInvocationMessage(invocationId));
        }

        private static CloseMessage CreateCloseMessage(ref MessagePackReader reader)
        {
            var error = ReadString(ref reader, "error");
            return new CloseMessage(error);
        }

        //private static StreamCompleteMessage CreateStreamCompleteMessage(ref MessagePackReader reader)
        //{
        //    var streamId = ReadString(ref reader, "streamId");
        //    var error = ReadString(ref reader, "error");
        //    if (string.IsNullOrEmpty(error))
        //    {
        //        error = null;
        //    }
        //    return new StreamCompleteMessage(streamId, error);
        //}

        private static Dictionary<string, string> ReadHeaders(ref MessagePackReader reader)
        {
            var headerCount = ReadMapLength(ref reader, "headers");
            if (headerCount > 0)
            {
                // If headerCount is larger than int.MaxValue, things are going to go horribly wrong anyway :)
                var headers = new Dictionary<string, string>((int)headerCount, StringComparer.Ordinal);

                for (var i = 0; i < headerCount; i++)
                {
                    var key = ReadString(ref reader, $"headers[{i}].Key");
                    var value = ReadString(ref reader, $"headers[{i}].Value");
                    headers[key] = value;
                }
                return headers;
            }
            else
            {
                return null;
            }
        }

        private static object[] BindArguments(ref MessagePackReader reader, IReadOnlyList<Type> parameterTypes, MessagePackSerializerOptions resolver)
        {
            var argumentCount = ReadArrayLength(ref reader, "arguments");

            if (parameterTypes.Count != argumentCount)
            {
                throw new InvalidDataException(
                    $"Invocation provides {argumentCount} argument(s) but target expects {parameterTypes.Count}.");
            }

            try
            {
                var arguments = new object[argumentCount];
                for (var i = 0; i < argumentCount; i++)
                {
                    arguments[i] = DeserializeObject(ref reader, parameterTypes[i], "argument", resolver);
                }

                return arguments;
            }
            catch (Exception ex)
            {
                throw new InvalidDataException("Error binding arguments. Make sure that the types of the provided values match the types of the hub method being invoked.", ex);
            }
        }

        private static T ApplyHeaders<T>(IDictionary<string, string> source, T destination) where T : HubInvocationMessage
        {
            if (source != null && source.Count > 0)
            {
                destination.Headers = source;
            }

            return destination;
        }

        /// <inheritdoc />
        public void WriteMessage(HubMessage message, IBufferWriter<byte> output)
        {
            var mem = MemoryBufferWriter.Get();
            var writer = new MessagePackWriter(mem);

            try
            {
                // Write message to a buffer so we can get its length
                WriteMessageCore(message, ref writer);
                
                // Write length then message to output
                BinaryMessageFormatter.WriteLengthPrefix(mem.Length, output);
                mem.CopyTo(output);
            }
            finally
            {
                MemoryBufferWriter.Return(mem);
            }
        }

        /// <inheritdoc />
        public ReadOnlyMemory<byte> GetMessageBytes(HubMessage message)
        {
            var mem = MemoryBufferWriter.Get();
            var writer = new MessagePackWriter(mem);

            try
            {
                // Write message to a buffer so we can get its length
                WriteMessageCore(message, ref writer);                
                var dataLength = mem.Length;
                var prefixLength = BinaryMessageFormatter.LengthPrefixLength(mem.Length);

                var array = new byte[dataLength + prefixLength];
                var span = array.AsSpan();

                // Write length then message to output
                var written = BinaryMessageFormatter.WriteLengthPrefix(mem.Length, span);
                Debug.Assert(written == prefixLength);
                mem.CopyTo(span.Slice(prefixLength));

                return array;
            }
            finally
            {
                MemoryBufferWriter.Return(mem);
            }
        }

        private void WriteMessageCore(HubMessage message, ref MessagePackWriter writer)
        {
            switch (message)
            {
                case InvocationMessage invocationMessage:
                    WriteInvocationMessage(invocationMessage, ref writer);
                    break;

                case StreamInvocationMessage streamInvocationMessage:
                    WriteStreamInvocationMessage(streamInvocationMessage, ref writer);
                    break;

                case StreamItemMessage streamItemMessage:
                    WriteStreamingItemMessage(streamItemMessage, writer);
                    break;

                case CompletionMessage completionMessage:
                    WriteCompletionMessage(completionMessage, ref writer);
                    break;

                case CancelInvocationMessage cancelInvocationMessage:
                    WriteCancelInvocationMessage(cancelInvocationMessage, ref writer);
                    break;

                case PingMessage pingMessage:
                    WritePingMessage(pingMessage, ref writer);
                    break;

                case CloseMessage closeMessage:
                    WriteCloseMessage(closeMessage, ref writer);
                    break;
                //case StreamCompleteMessage m:
                //    WriteStreamCompleteMessage(m, packer);
                //    break;
                default:
                    throw new InvalidDataException($"Unexpected message type: {message.GetType().Name}");
            }
            writer.Flush();
        }

        private void WriteInvocationMessage(InvocationMessage message, ref MessagePackWriter writer)
        {
            writer.WriteArrayHeader(5);
            writer.WriteInt32(HubProtocolConstants.InvocationMessageType);
            PackHeaders(ref writer, message.Headers);
            if (string.IsNullOrEmpty(message.InvocationId))
            {
                writer.WriteNil();
            }
            else
            {
                writer.Write(message.InvocationId);
            }
            writer.Write(message.Target);
            writer.WriteArrayHeader(message.Arguments.Length);
            foreach (var arg in message.Arguments)
            {
                WriteArgument(arg, ref writer);
            }
        }

        private void WriteStreamInvocationMessage(StreamInvocationMessage message, ref MessagePackWriter writer)
        {
            writer.WriteArrayHeader(5);
            writer.WriteInt16(HubProtocolConstants.StreamInvocationMessageType);
            PackHeaders(ref writer, message.Headers);
            writer.Write(message.InvocationId);
            writer.Write(message.Target);

            writer.WriteArrayHeader(message.Arguments.Length);
            foreach (var arg in message.Arguments)
            {
                WriteArgument(arg, ref writer);
            }
        }

        private void WriteStreamingItemMessage(StreamItemMessage message, MessagePackWriter writer)
        {
            writer.WriteArrayHeader(4);
            writer.WriteInt16(HubProtocolConstants.StreamItemMessageType);
            PackHeaders(ref writer, message.Headers);
            writer.Write(message.InvocationId);
            WriteArgument(message.Item, ref writer);
        }

        private void WriteArgument(object argument, ref MessagePackWriter writer)
        {
            if (argument == null)
            {
                writer.WriteNil();
            }
            else
            {
                MessagePackSerializer.Serialize(argument.GetType(), ref writer, argument, _options);
            }
        }

        private void WriteCompletionMessage(CompletionMessage message, ref MessagePackWriter writer)
        {
            var resultKind =
                message.Error != null ? ErrorResult :
                message.HasResult ? NonVoidResult :
                VoidResult;

            writer.WriteArrayHeader(4 + (resultKind != VoidResult ? 1 : 0));
            writer.WriteInt32(HubProtocolConstants.CompletionMessageType);
            PackHeaders(ref writer, message.Headers);
            writer.Write(message.InvocationId);
            writer.WriteInt32(resultKind);
            switch (resultKind)
            {
                case ErrorResult:
                    writer.Write(message.Error);
                    break;

                case NonVoidResult:
                    WriteArgument(message.Result, ref writer);
                    break;
            }
        }

        private void WriteCancelInvocationMessage(CancelInvocationMessage message, ref MessagePackWriter writer)
        {
            writer.WriteArrayHeader(3);
            writer.WriteInt16(HubProtocolConstants.CancelInvocationMessageType);
            PackHeaders(ref writer, message.Headers);
            writer.Write(message.InvocationId);
        }

        //private void WriteStreamCompleteMessage(StreamCompleteMessage message, ref MessagePackWriter writer)
        //{
        //    writer.WriteArrayHeader(3);
        //    writer.WriteInt16(HubProtocolConstants.StreamCompleteMessageType);
        //    writer.Write(message.StreamId);
        //    if (message.HasError)
        //    {
        //        writer.Write(message.Error);
        //    }
        //    else
        //    {
        //        writer.WriteNil();
        //    }
        //}

        private void WriteCloseMessage(CloseMessage message, ref MessagePackWriter writer)
        {
            writer.WriteArrayHeader(2);
            writer.WriteInt16(HubProtocolConstants.CloseMessageType);
            if (string.IsNullOrEmpty(message.Error))
            {
                writer.WriteNil();
            }
            else
            {
                writer.Write(message.Error);
            }
        }

        private void WritePingMessage(PingMessage pingMessage, ref MessagePackWriter writer)
        {
            writer.WriteArrayHeader(1);
            writer.WriteInt32(HubProtocolConstants.PingMessageType);
        }

        private void PackHeaders(ref MessagePackWriter writer, IDictionary<string, string> headers)
        {
            if (headers != null)
            {
                writer.WriteMapHeader(headers.Count);
                if (headers.Count > 0)
                {
                    foreach (var header in headers)
                    {
                        writer.Write(header.Key);
                        writer.Write(header.Value);
                    }
                }
            }
            else
            {
                writer.WriteMapHeader(0);
            }
        }

        private static string ReadInvocationId(ref MessagePackReader reader)
        {
            return ReadString(ref reader, "invocationId");
        }

        private static int ReadInt32(ref MessagePackReader reader, string field)
        {
            Exception msgPackException = null;
            try
            {
                var readInt = reader.ReadInt32();
                return readInt;
            }
            catch (Exception e)
            {
                msgPackException = e;
            }

            throw new InvalidDataException($"Reading '{field}' as Int32 failed.", msgPackException);
        }

        private static string ReadString(ref MessagePackReader reader, string field)
        {
            Exception msgPackException = null;
            try
            {
                var readString = reader.ReadString();

                return readString;
            }
            catch (Exception e)
            {
                msgPackException = e;
            }

            throw new InvalidDataException($"Reading '{field}' as String failed.", msgPackException);
        }

        private static bool ReadBoolean(ref MessagePackReader reader, string field)
        {
            Exception msgPackException = null;
            try
            {
                var readBool = reader.ReadBoolean();
                return readBool;
            }
            catch (Exception e)
            {
                msgPackException = e;
            }

            throw new InvalidDataException($"Reading '{field}' as Boolean failed.", msgPackException);
        }

        private static long ReadMapLength(ref MessagePackReader reader, string field)
        {
            Exception msgPackException = null;
            try
            {
                var readMap = reader.ReadMapHeader();

                return readMap;
            }
            catch (Exception e)
            {
                msgPackException = e;
            }

            throw new InvalidDataException($"Reading map length for '{field}' failed.", msgPackException);
        }

        private static long ReadArrayLength(ref MessagePackReader reader, string field)
        {
            Exception msgPackException = null;
            try
            {
                var readArray = reader.ReadArrayHeader();

                return readArray;
            }
            catch (Exception e)
            {
                msgPackException = e;
            }

            throw new InvalidDataException($"Reading array length for '{field}' failed.", msgPackException);
        }

        private static object DeserializeObject(ref MessagePackReader reader, Type type, string field, MessagePackSerializerOptions options)
        {
            Exception msgPackException = null;
            try
            {
                var obj = MessagePackSerializer.Deserialize(type, ref reader, options);

                return obj;
            }
            catch (Exception ex)
            {
                msgPackException = ex;
            }

            throw new InvalidDataException($"Deserializing object of the `{type.Name}` type for '{field}' failed.", msgPackException);
        }

        internal static List<IFormatterResolver> CreateDefaultFormatterResolvers()
        {
            // Copy to allow users to add/remove resolvers without changing the static SignalRResolver list
            return new List<IFormatterResolver>(SignalRResolver.Resolvers);
        }

        internal class SignalRResolver : IFormatterResolver
        {
            public static readonly IFormatterResolver Instance = new SignalRResolver();

            public static readonly IList<IFormatterResolver> Resolvers = new List<IFormatterResolver>
            {
                MessagePack.Resolvers.DynamicEnumAsStringResolver.Instance,
                MessagePack.Resolvers.ContractlessStandardResolver.Instance
            };

            public IMessagePackFormatter<T> GetFormatter<T>()
            {
                return Cache<T>.Formatter;
            }

            private static class Cache<T>
            {
                public static readonly IMessagePackFormatter<T> Formatter;

                static Cache()
                {
                    foreach (var resolver in Resolvers)
                    {
                        Formatter = resolver.GetFormatter<T>();
                        if (Formatter != null)
                        {
                            return;
                        }
                    }
                }
            }
        }

        // Support for users making their own Formatter lists
        internal class CombinedResolvers : IFormatterResolver
        {
            private readonly IList<IFormatterResolver> _resolvers;

            public CombinedResolvers(IList<IFormatterResolver> resolvers)
            {
                _resolvers = resolvers;
            }

            public IMessagePackFormatter<T> GetFormatter<T>()
            {
                foreach (var resolver in _resolvers)
                {
                    var formatter = resolver.GetFormatter<T>();
                    if (formatter != null)
                    {
                        return formatter;
                    }
                }

                return null;
            }
        }
    }
}