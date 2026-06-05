using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;

namespace CollectorModels.ValueConverter;

public class EnumCollectionJsonValueConverter<T> : ValueConverter<IEnumerable<T>, string> where T : Enum
{
    public EnumCollectionJsonValueConverter() : base(
        v => JsonConvert.SerializeObject(v.Select(e => e.ToString())),
        v => JsonConvert.DeserializeObject<IEnumerable<string>>(v).Select(e => (T)Enum.Parse(typeof(T), e)).ToArray())
    {
    }
}
