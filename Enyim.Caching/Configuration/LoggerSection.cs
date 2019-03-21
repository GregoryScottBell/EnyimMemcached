using System;
using System.Configuration;
using System.ComponentModel;

namespace Enyim.Caching.Configuration
{
	public class LoggerSection : ConfigurationSection
	{
		[ConfigurationProperty("factory", IsRequired = true)]
		[InterfaceValidator(typeof(ILogFactory)), TypeConverter(typeof(TypeNameConverter))]
		public Type LogFactory
		{
			get { return (Type)base["factory"]; }
			set { base["factory"] = value; }
		}
	}
}
