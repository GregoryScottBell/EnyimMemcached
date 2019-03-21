using System;

namespace Enyim.Caching.Memcached
{
	public interface IPerformanceMonitor : IDisposable
	{
		void Get(int amount, bool success);
		void Store(StoreMode mode, int amount, bool success);
		void Delete(int amount, bool success);
		void Mutate(MutationMode mode, int amount, bool success);
		void Concatenate(ConcatenationMode mode, int amount, bool success);
	}
}
