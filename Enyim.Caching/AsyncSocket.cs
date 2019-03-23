using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using AsyncUtilities;
using Enyim.Caching.Memcached.Protocol.Binary;
using Enyim.Caching.Memcached.Results;

namespace Enyim.Caching
{
	[StructLayout(LayoutKind.Sequential, Size = 24, Pack = 1)]
	internal struct RequestHeader
	{
		public byte magic;
		public OpCode opcode;
		public ushort keyLength;
		public byte extrasLength;
		public byte dataType;
		public ushort vBucketId;
		public uint totalBodyLength;
		public uint opaque;
		public ulong cas;
	}

	[StructLayout(LayoutKind.Sequential, Size = 24, Pack = 1)]
	internal struct ResponseHeader
	{
		public byte magic;
		public OpCode opcode;
		public ushort keyLength;
		public byte extrasLength;
		public byte dataType;
		public ushort status;
		public uint totalBodyLength;
		public uint opaque;
		public ulong cas;
	}

	internal class AsyncSocket
	{
		private readonly ConcurrentBag<SocketAwaitable> awaitablePool = new ConcurrentBag<SocketAwaitable>();
		private readonly SocketAwaitable[] waiters;
		private readonly ConcurrentBag<byte[]> headerBufferPool;
		private readonly int maxConcurrentCommands;
		private readonly Socket socket;
		private readonly SemaphoreSlim semaphore;
		private readonly AsyncLock writeLock = new AsyncLock();
		private int opaque;


		public AsyncSocket(int maxConcurrentCommands, Socket socket)
		{
			if (maxConcurrentCommands <= 0)
				throw new ArgumentOutOfRangeException(nameof(maxConcurrentCommands), maxConcurrentCommands, "Must greater than 0.");
			
			this.maxConcurrentCommands = maxConcurrentCommands;
			this.socket = socket ?? throw new ArgumentNullException(nameof(socket));
			this.semaphore = new SemaphoreSlim(maxConcurrentCommands, maxConcurrentCommands);
			this.headerBufferPool = new ConcurrentBag<byte[]>(Enumerable.Range(0, maxConcurrentCommands).Select(_ => new byte[24]));
			this.waiters = new SocketAwaitable[maxConcurrentCommands];

			foreach (var _ in Enumerable.Range(0, maxConcurrentCommands))
			{
				this.awaitablePool.Add(new SocketAwaitable(this));
			}

			var reader = new SocketAwaitable(this);
			reader.eventArgs.SetBuffer(new byte[24], 0, 24);
			reader.Reset();
			reader.wasCompleted = socket.ReceiveAsync(reader.eventArgs);
			reader.OnCompleted(reader.WakeNextReader);
		}

		public async Task<short> NoOp()
		{
			await this.semaphore.WaitAsync();
			this.awaitablePool.TryTake(out var awaitable);
			Debug.Assert(awaitable != null, $"{nameof(awaitable)} cannot be null.");

			this.headerBufferPool.TryTake(out var buffer);
			Debug.Assert(awaitable != null, $"{nameof(awaitable)} cannot be null.");

			awaitable.eventArgs.SetBuffer(buffer, 0, 24);
			int opaque;
			using (await writeLock.LockAsync())
			{
				opaque = Interlocked.Increment(ref this.opaque);
				unsafe
				{
					fixed (byte* bytes = buffer)
					{
						int* ints = (int*)bytes;
						bytes[0] = 0x80;
						bytes[1] = (byte)OpCode.NoOp;
						ints[3] = opaque;
					}
				}

				Debug.Assert(this.waiters[opaque % this.maxConcurrentCommands] == null, "We're stomping someone else's slot.");
				this.waiters[opaque % this.maxConcurrentCommands] = awaitable;
				awaitable.wasCompleted = !this.socket.SendAsync(awaitable.eventArgs);
				await awaitable;
			}
			this.headerBufferPool.Add(awaitable.eventArgs.Buffer);
			awaitable.Reset();

			await awaitable;

			short result;
			unsafe
			{
				fixed (byte* bytes = awaitable.eventArgs.Buffer)
				{
					int* ints = (int*)bytes;
					Debug.Assert(ints[3] == opaque);
					short* shorts = (short*)bytes;
					result = shorts[3];
				}
			}
			awaitable.Reset();
			awaitable.wasCompleted = socket.ReceiveAsync(awaitable.eventArgs);
			awaitable.OnCompleted(awaitable.WakeNextReader);
			this.semaphore.Release();
			return result;
		}

		private sealed class SocketAwaitable : INotifyCompletion
		{
			private readonly static Action SENTINEL = () => { };

			private readonly AsyncSocket socket;

			internal bool wasCompleted;
			internal Action continuation;
			internal SocketAsyncEventArgs eventArgs;

			internal SocketAwaitable(AsyncSocket sock)
			{
				this.socket = sock ?? throw new ArgumentNullException(nameof(sock));
				this.eventArgs = new SocketAsyncEventArgs { UserToken = this, RemoteEndPoint = sock.socket.RemoteEndPoint, };

				eventArgs.Completed += (_, args) =>
				{
					var awaitable = args.UserToken as SocketAwaitable;
					(awaitable.continuation ?? Interlocked.CompareExchange(ref awaitable.continuation, SENTINEL, null))?.Invoke();
				};
			}

			public void Reset()
			{
				this.wasCompleted = false;
				this.continuation = null;
			}

			public SocketAwaitable GetAwaiter() => this;

			public bool IsCompleted => wasCompleted;

			public void OnCompleted(Action continuation)
			{
				if (this.continuation == SENTINEL
					|| Interlocked.CompareExchange(ref this.continuation, continuation, null) == SENTINEL)
				{
					Task.Run(continuation);
				}
			}

			public void GetResult()
			{
				if (eventArgs.SocketError != SocketError.Success)
					throw new SocketException((int)eventArgs.SocketError);
			}

			public void WakeNextReader()
			{
				int opaque;
				unsafe
				{
					fixed (byte* bytes = this.eventArgs.Buffer)
					{
						int* ints = (int*)bytes;
						opaque = ints[3];
					}
				}

				var awaitable = this.socket.waiters[opaque % this.socket.maxConcurrentCommands];
				awaitable.eventArgs.SetBuffer(this.eventArgs.Buffer, 0, 24);
				this.Reset();
				this.socket.awaitablePool.Add(this);
			}
		}
	}
}
