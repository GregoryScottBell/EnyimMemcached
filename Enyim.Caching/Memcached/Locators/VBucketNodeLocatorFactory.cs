using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Enyim.Caching.Configuration;
using System.IO;
using System.Runtime.Serialization.Json;

namespace Enyim.Caching.Memcached
{
	/// <summary>
	/// Factory for the vbucket based locator.
	/// </summary>
	/// <remarks>You need to use this in the configuration file because this is the only way pass parameters to the VBucketNodeLocator.
	/// 
	///		<locator factory="Enyim.Caching.Memcached.VBucketNodeLocatorFactory" configFile="vbucket.json" />
	/// 
	/// </remarks>
	public class VBucketNodeLocatorFactory : IProviderFactory<IMemcachedNodeLocator>
	{
		private string hashAlgo;
		private VBucket[] buckets;

		void IProvider.Initialize(Dictionary<string, string> parameters)
		{
			ConfigurationHelper.TryGetAndRemove(parameters, "hashAlgorithm", out this.hashAlgo, true);

			ConfigurationHelper.TryGetAndRemove(parameters, String.Empty, out string json, true);
			ConfigurationHelper.CheckForUnknownAttributes(parameters);

			var tmp = (int[][]) new DataContractJsonSerializer(typeof(int[][]))
				.ReadObject(new MemoryStream(new UTF8Encoding(false).GetBytes(json)));

			this.buckets = tmp.Select(entry => new VBucket(entry[0], entry.Skip(1).ToArray())).ToArray();
		}

		IMemcachedNodeLocator IProviderFactory<IMemcachedNodeLocator>.Create()
		{
			return new VBucketNodeLocator(this.hashAlgo, this.buckets);
		}
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
