# Wima.AspNetCore.SignalR.Protocols.MessagePack
A MessagePack 2.x port of Microsoft.AspNetCore.SignalR.Protocols.MessagePack. 


For Microsoft.AspNetCore.SignalR.Protocols.MessagePack 3.1.2 (so far) doesn't work with MessagePack C# 2.x, as you can find on nuget.org, i rewrote the code to adapt the MessagePack 2.x API.

The issue: If you add Microsoft.AspNetCore.SignalR.Protocols.MessagePack 3.1.2 to your project dependency,especially in a netstandard project, you may encounter the exception like "Could not find the field:"xxxx.Instance". However in an aspnetcore project, Nuget seems to pull the correct 1.x version of MessagePack instead and work normally.

Anyway, this port of Microsoft.AspNetCore.SignalR.Protocols.MessagePack may give your project support to MessagePack 2.x.

Note: Due to Signature problem, the AssemblyName must be renamed as to use the compiled library with other packages from .net core (Under Microsoft.xxx namespace).


Original Codebase:https://github.com/aspnet/SignalR/tree/master/src/Microsoft.AspNetCore.SignalR.Protocols.MessagePack
