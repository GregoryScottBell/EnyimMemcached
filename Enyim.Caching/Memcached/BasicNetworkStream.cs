//#define DEBUG_IO
using System;
using System.IO;
using System.Net.Sockets;

namespace Enyim.Caching.Memcached
{
	public partial class PooledSocket
	{
		#region [ BasicNetworkStream           ]

		private class BasicNetworkStream : Stream
		{
			private readonly Socket socket;

			public BasicNetworkStream(Socket socket)
			{
				this.socket = socket;
			}

			public override bool CanRead => true;

			public override bool CanSeek => false;

			public override bool CanWrite => false;

			public override void Flush()
			{
			}

			public override long Length => throw new NotSupportedException();

			public override long Position
			{
				get { throw new NotSupportedException(); }
				set { throw new NotSupportedException(); }
			}

			public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
			{

				var retval = this.socket.BeginReceive(buffer, offset, count, SocketFlags.None, out SocketError errorCode, callback, state);

				if (errorCode == SocketError.Success)
					return retval;

				throw new System.IO.IOException(String.Format("Failed to read from the socket '{0}'. Error: {1}", this.socket.RemoteEndPoint, errorCode));
			}

			public override int EndRead(IAsyncResult asyncResult)
			{
				var retval = this.socket.EndReceive(asyncResult, out SocketError errorCode);

				// actually "0 bytes read" could mean an error as well
				if (errorCode == SocketError.Success && retval > 0)
					return retval;

				throw new System.IO.IOException(String.Format("Failed to read from the socket '{0}'. Error: {1}", this.socket.RemoteEndPoint, errorCode));
			}

			public override int Read(byte[] buffer, int offset, int count)
			{
				int retval = this.socket.Receive(buffer, offset, count, SocketFlags.None, out SocketError errorCode);

				// actually "0 bytes read" could mean an error as well
				if (errorCode == SocketError.Success && retval > 0)
					return retval;

				throw new System.IO.IOException(String.Format("Failed to read from the socket '{0}'. Error: {1}", this.socket.RemoteEndPoint, errorCode == SocketError.Success ? "?" : errorCode.ToString()));
			}

			public override long Seek(long offset, SeekOrigin origin)
			{
				throw new NotSupportedException();
			}

			public override void SetLength(long value)
			{
				throw new NotSupportedException();
			}

			public override void Write(byte[] buffer, int offset, int count)
			{
				throw new NotSupportedException();
			}
		}

		#endregion
	}
}

#region [ License information          ]
/* ************************************************************
 *
 *    Copyright (c) 2010 Attila Kiskó, enyim.com
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 * ************************************************************/
#endregion
