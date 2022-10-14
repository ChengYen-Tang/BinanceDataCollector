using Microsoft.EntityFrameworkCore.ChangeTracking;
using System;
using System.Collections.Generic;
using System.Linq;

namespace CollectorModels;

public class CollectionValueComparer<T> : ValueComparer<IEnumerable<T>>
{
    public CollectionValueComparer() : base((c1, c2) => c1.SequenceEqual(c2),
        c => c.Aggregate(0, (a, v) => HashCode.Combine(a, v.GetHashCode())), c => c.ToHashSet())
    {
    }
}
