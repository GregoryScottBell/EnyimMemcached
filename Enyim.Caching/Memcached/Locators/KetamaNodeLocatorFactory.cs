using System.Collections.Generic;
using Enyim.Caching.Configuration;

namespace Enyim.Caching.Memcached
{
	/// <summary>
	/// Implements Ketama cosistent hashing, compatible with the "spymemcached" Java client
	/// </summary>
	public class KetamaNodeLocatorFactory : IProviderFactory<IMemcachedNodeLocator>
	{
		private string hashName;

		void IProvider.Initialize(Dictionary<string, string> parameters)
		{
			ConfigurationHelper.TryGetAndRemove(parameters, "hashName", out this.hashName, false);
		}

		IMemcachedNodeLocator IProviderFactory<IMemcachedNodeLocator>.Create()
		{
			return new KetamaNodeLocator(this.hashName);
		}
	}
}

#region [ License information          ]
/* ************************************************************
 * 
 *    Copyright (c) 2010 Attila Kisk�, enyim.com
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
