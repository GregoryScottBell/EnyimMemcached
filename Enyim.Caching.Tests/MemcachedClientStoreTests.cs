﻿using NUnit.Framework;
using Enyim.Caching.Memcached;
using Enyim.Caching.Memcached.Results.StatusCodes;

namespace Enyim.Caching.Tests
{

	[TestFixture(Description = "MemcachedClient Store Tests")]
	public class MemcachedClientStoreTests : MemcachedClientTestsBase
	{

		[Test]
		public void When_Storing_Item_With_New_Key_And_StoreMode_Add_Result_Is_Successful()
		{
			var result = Store(StoreMode.Add);
			StoreAssertPass(result);

		}

		[Test]
		public void When_Storing_Item_With_Existing_Key_And_StoreMode_Add_Result_Is_Not_Successful()
		{
			var key = GetUniqueKey("store");
			var result = Store(StoreMode.Add, key);
			StoreAssertPass(result);

			result = Store(StoreMode.Add, key);
			StoreAssertFail(result);
		}

		[Test]
		public void When_Storing_Item_With_New_Key_And_StoreMode_Replace_Result_Is_Not_Successful()
		{
			var result = Store(StoreMode.Replace);
			Assert.That(result.StatusCode, Is.EqualTo((int)StatusCodeEnums.NotFound), "Invalid status code");
			StoreAssertFail(result);

		}

		[Test]
		public void When_Storing_Item_With_Existing_Key_And_StoreMode_Replace_Result_Is_Successful()
		{
			var key = GetUniqueKey("store");
			var result = Store(StoreMode.Add, key);
			StoreAssertPass(result);

			result = Store(StoreMode.Replace, key);
			StoreAssertPass(result);
		}

		[Test]
		public void When_Storing_Item_With_New_Key_And_StoreMode_Set_Result_Is_Successful()
		{
			var result = Store(StoreMode.Set);
			StoreAssertPass(result);
		}

		[Test]
		public void When_Storing_Item_With_Existing_Key_And_StoreMode_Set_Result_Is_Successful()
		{
			var key = GetUniqueKey("store");
			var result = Store(StoreMode.Add, key);
			StoreAssertPass(result);

			result = Store(StoreMode.Set, key);
			StoreAssertPass(result);
		}
	}
}

#region [ License information          ]
/* ************************************************************
 *
 *    @author Couchbase <info@couchbase.com>
 *    @copyright 2012 Couchbase, Inc.
 *    @copyright 2012 Attila Kiskó, enyim.com
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
