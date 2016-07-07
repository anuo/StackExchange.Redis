using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;


namespace Study.StackExchange.Redis
{
    public class RedisHelper
    {
        public static IDatabase Redis = null;
        public RedisHelper()
        {
        }

        /// <summary>
        /// 保持 Redis 连接存活
        /// </summary>
        public static void RedisConnectionKeepAlive()
        {
            if (Redis == null)
            {
                bool IsRedisCluster = false;
                string virtualIp = "172.16.131.61";
                ConfigurationOptions config = null;
                if (IsRedisCluster) //集群
                {
                    if (string.IsNullOrEmpty(virtualIp))
                    {
                         
                    }

                    config = new ConfigurationOptions()
                    {
                        EndPoints =
                        {
                            {virtualIp,6377 },
                        },

                        Proxy = Proxy.Twemproxy,//用Twemproxy 可以减少连接数, 并且方便扩展 redis 集群节点
                        ConnectTimeout = 6000,
                        //KeepAlive = 180, //一般不用,  默认值为 -1 
                        AbortOnConnectFail = true,
                        DefaultDatabase = 0 //用了 Twemproxy 这里只能为0 , 也建议用0, 而不是用db1,db2 等多个数据库 见: http://stackoverflow.com/questions/13386053/how-do-i-change-between-redis-database
                    };


                }
                else //单机
                {
                    config = new ConfigurationOptions()
                    {
                        EndPoints =
                        {
                            {virtualIp,6379 },
                        },

                        ConnectTimeout = 3000,
                        //KeepAlive = 180, //一般不用,  默认值为 -1 
                        AbortOnConnectFail = true,
                        DefaultDatabase = 0,

                    };

                }


                Task<ConnectionMultiplexer> conCheck = ConnectionMultiplexer.ConnectAsync(config); //连接字符串检查
                Thread.Sleep(3000);
                if (conCheck.Status == TaskStatus.RanToCompletion)
                {
                    //连接字符串有效, 开始连接
                    ConnectionMultiplexer con = ConnectionMultiplexer.Connect(config);
                    Redis = con.GetDatabase();
                }
                else
                {
                    //连接字符串无效, 记录日志
                    //var logger = GlobalLogger.GetLogger("SOLAR", LoggerType.ExceptionLog);
                    //logger.Error("redis连接超时, 连接字符串为:" + config);
                }

            }
        }

        /// <summary>
        /// 根据传入的类型, 返回一个集合 (来源于redis hash 数据)
        /// </summary>
        /// <typeparam name="T">集合类型</typeparam>
        /// <param name="key">redis key</param>
        /// <param name="list">集合</param>
        /// <returns></returns>
        public static IList<T> RedisHashGet<T>(string key, IList<T> list) where T : new()
        {
            if (Redis.KeyExists(key))
            {
                HashEntry[] hashArray = Redis.HashGetAll(key);
                foreach (var item in hashArray)
                {
                    list.Add(JsonConvert.DeserializeObject<T>(item.Value));
                }
            }

            if (list == null)
            {
                list = new List<T>();
            }

            return list;
        }

        /// <summary>
        /// 将传入的集合,以hash 类型保存,fieldValue为一个json对象
        /// </summary>
        /// <typeparam name="T">集合类型</typeparam>
        /// <param name="key">redis key</param>
        /// <param name="list">集合</param>
        /// <param name="fieldNameKey">将指定的fieldNameKey值作为fieldName</param>
        public static void RedisHashSet<T>(string key, List<T> list, string fieldNameKey) where T : new()
        {
            List<HashEntry> listHash = new List<HashEntry>();
            foreach (var item in list)
            {
                string fieldName = ReflectionGetPropertyValue(item, fieldNameKey);
                string fieldValue = JsonConvert.SerializeObject(item);
                HashEntry he = new HashEntry(fieldName, fieldValue);
                listHash.Add(he);
            }

            Redis.HashSet(key, listHash.ToArray());

            //默认三天过期
            Redis.KeyExpire(key, DateTime.Now.AddDays(3));
        }

        /// <summary>
        /// 将传入的集合,以hash 类型保存, fieldValue为一个字符串
        /// </summary>
        /// <typeparam name="T">集合类型</typeparam>
        /// <param name="key">redis key</param>
        /// <param name="list">集合</param>
        /// <param name="fieldNameKey">将指定的fieldNameKey值作为fieldName</param>
        /// <param name="fieldValueKey">将指定的fieldValueKey值作为fieldValue</param>
        public static void RedisHashSet<T>(string key, List<T> list, string fieldNameKey, string fieldValueKey) where T : new()
        {
            List<HashEntry> listHash = new List<HashEntry>();
            foreach (var item in list)
            {
                string fieldName = ReflectionGetPropertyValue(item, fieldNameKey);
                string fieldValue = ReflectionGetPropertyValue(item, fieldValueKey);
                HashEntry he = new HashEntry(fieldName, fieldValue);
                listHash.Add(he);
            }

            Redis.HashSet(key, listHash.ToArray());

            //默认三天过期
            Redis.KeyExpire(key, DateTime.Now.AddDays(3));

        }

        /// <summary>
        /// 在hash数据集合里面, 将传入的集合从中删除 (根据fieldName)
        /// </summary>
        /// <typeparam name="T">集合类型</typeparam>
        /// <param name="key">redis key</param>
        /// <param name="list">集合</param>
        /// <param name="fieldNameKey">将指定的fieldNameKey值作为fieldName</param>
        public static void RedisHashDelete<T>(string key, List<T> list, string fieldNameKey) where T : new()
        {
            List<RedisValue> listHash = new List<RedisValue>();
            foreach (var item in list)
            {
                string fieldName = ReflectionGetPropertyValue(item, fieldNameKey);
                listHash.Add(fieldName);
            }

            Redis.HashDelete(key, listHash.ToArray());
        }

        /// <summary>
        /// 获取类中指定属性名的值
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="obj">类</param>
        /// <param name="propertyName">属性名</param>
        /// <returns>属性值</returns>
        public static string ReflectionGetPropertyValue<T>(T obj, string propertyName) where T : new()
        {
            var property = obj.GetType().GetProperty(propertyName);
            if (property != null)
            {
                if (property.GetValue(obj) == null)
                {
                    return "";
                }
                else
                {
                    return property.GetValue(obj).ToString();
                }
            }
            else
            {
                throw new Exception(string.Format("在[{0}]类中,没有找到名称为[{1}]的属性", obj.GetType().Name, propertyName));
            }
        }
    }
}
