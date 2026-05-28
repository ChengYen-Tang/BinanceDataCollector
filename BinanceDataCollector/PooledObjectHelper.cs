using Microsoft.Extensions.ObjectPool;
using System.Collections.Concurrent;

namespace BinanceDataCollector;

internal static class PooledObjectHelper
{
    private const int MaxPoolCapacityBucket = 16_384;
    private static readonly DefaultObjectPoolProvider PoolProvider = new();
    private static readonly ConcurrentDictionary<(Type ItemType, int InitialCapacity), object> ListPools = [];
    private static readonly ConcurrentDictionary<(Type KeyType, Type ValueType, int InitialCapacity), object> DictionaryPools = [];

    public static ObjectPool<List<T>> GetListPool<T>(int initialCapacity)
        => (ObjectPool<List<T>>)ListPools.GetOrAdd(
            (typeof(T), NormalizeInitialCapacity(initialCapacity)),
            static key => PoolProvider.Create(new PooledListPolicy<T>(key.InitialCapacity)));

    public static List<T> RentList<T>(int initialCapacity)
        => GetListPool<T>(initialCapacity).Get();

    public static void ReturnList<T>(List<T> rows, int initialCapacity)
        => GetListPool<T>(initialCapacity).Return(rows);

    public static ObjectPool<Dictionary<TKey, TValue>> GetDictionaryPool<TKey, TValue>(int initialCapacity)
        where TKey : notnull
        => (ObjectPool<Dictionary<TKey, TValue>>)DictionaryPools.GetOrAdd(
            (typeof(TKey), typeof(TValue), NormalizeInitialCapacity(initialCapacity)),
            static key => PoolProvider.Create(new PooledDictionaryPolicy<TKey, TValue>(key.InitialCapacity)));

    private static int NormalizeInitialCapacity(int requestedCapacity)
    {
        if (requestedCapacity <= 0)
            return 0;

        int capacity = 1;
        while (capacity < requestedCapacity && capacity < MaxPoolCapacityBucket)
            capacity <<= 1;

        return capacity;
    }

    private sealed class PooledListPolicy<T>(int initialCapacity) : PooledObjectPolicy<List<T>>
    {
        public override List<T> Create()
            => new(initialCapacity);

        public override bool Return(List<T> obj)
        {
            obj.Clear();
            return true;
        }
    }

    private sealed class PooledDictionaryPolicy<TKey, TValue>(int initialCapacity) : PooledObjectPolicy<Dictionary<TKey, TValue>>
        where TKey : notnull
    {
        public override Dictionary<TKey, TValue> Create()
            => new(initialCapacity);

        public override bool Return(Dictionary<TKey, TValue> obj)
        {
            obj.Clear();
            return true;
        }
    }
}
