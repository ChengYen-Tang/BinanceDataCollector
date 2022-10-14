using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Linq;

namespace CollectorModels.ValueConverter;

public class StringCollectionJsonValueConverter : ValueConverter<IEnumerable<string>, string>
{
    public StringCollectionJsonValueConverter() : base(
        v => JsonConvert.SerializeObject(v),
        v => JsonConvert.DeserializeObject<IEnumerable<string>>(v).ToArray())
    {
    }
}
