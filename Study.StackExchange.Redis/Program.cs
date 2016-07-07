using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Study.StackExchange.Redis
{
    class Program
    {
        static void Main(string[] args)
        {
            RedisHelper.RedisConnectionKeepAlive();
            //添加
            List<product> plist = new List<product>();
            plist.Add(new product { name = "电脑", area = "成都" });
            plist.Add(new product { name = "手机", area = "四川" });

            RedisHelper.RedisHashSet("product.redis", plist, "name");

            Console.ReadKey();
        }
    }

    class product
    {
        public string name { get; set; }
        public string area { get; set; }
    }
}
