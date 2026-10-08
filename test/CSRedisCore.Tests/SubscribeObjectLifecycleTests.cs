using CSRedis;
using CSRedis.Internal.ObjectPool;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace CSRedisCore.Tests
{
    /// <summary>
    /// SubscribeObject 生命周期测试：不依赖真实 Redis（fake 池 + null 连接值）。
    /// 注意：连接值为 null，订阅线程若启动会在 conn.Value 事件挂接处
    /// （conn.Value.SubscriptionReceived += ...）因未处理 NRE 立即死亡，
    /// 该挂接先于 while 循环与 Ping、位于任何 try 之前，线程不会进入 3s 重试循环；
    /// 因此"未调用 Start 前不启动线程"与"Dispose 后连接只归还一次"是可断言的确定性边界。
    /// </summary>
    public class SubscribeObjectLifecycleTests
    {
        private class FakePool : IObjectPool<RedisClient>
        {
            public List<(Object<RedisClient> obj, bool isReset)> Returned = new List<(Object<RedisClient>, bool)>();
            public int ReturnCount { get { lock (Returned) return Returned.Count; } }

            public IPolicy<RedisClient> Policy => throw new NotImplementedException();
            public bool IsAvailable => true;
            public Exception UnavailableException => null;
            public DateTime? UnavailableTime => null;
            public string Statistics => string.Empty;
            public string StatisticsFullily => string.Empty;
            public bool SetUnavailable(Exception exception, DateTime lastGetTime) => false;
            public Object<RedisClient> Get(TimeSpan? timeout = null) => CSRedis.Internal.ObjectPool.Object<RedisClient>.InitWith(this, 1, null);
            public Task<Object<RedisClient>> GetAsync() => Task.FromResult(Get());
            public void Return(Object<RedisClient> obj, bool isReset = false) { lock (Returned) Returned.Add((obj, isReset)); }
            public void Dispose() { }
        }

        private static CSRedisClient.SubscribeObject CreateUnstarted(FakePool pool)
        {
            var conn = CSRedis.Internal.ObjectPool.Object<RedisClient>.InitWith(pool, 1, null);
            var subscr = (chans: new[] { "chan1" }, conn: conn);
            return new CSRedisClient.SubscribeObject(
                redis: null,
                channels: new[] { "chan1" },
                subscrs: new[] { subscr },
                onMessageDic: new Dictionary<string, Action<CSRedisClient.SubscribeMessageEventArgs>>(),
                logger: new LoggerFactory().CreateLogger<CSRedisClient.SubscribeObject>());
        }

        [Fact]
        public void Construct_WithoutStart_DoesNotReturnConnection()
        {
            var pool = new FakePool();
            var so = CreateUnstarted(pool);
            Thread.Sleep(200); // 旧实现在构造函数内启动线程，此窗口内若已启动则状态不确定
            Assert.Equal(0, pool.ReturnCount);
            Assert.False(so.IsUnsubscribed);
        }

        [Fact]
        public void Start_Dispose_ReturnsConnectionExactlyOnce()
        {
            var pool = new FakePool();
            var so = CreateUnstarted(pool);
            so.Start();
            so.Dispose();
            Assert.True(so.IsUnsubscribed);
            Thread.Sleep(200); // 给订阅线程退出时间（Value 为 null，线程在求值 IsUnsubscribed 之前已于 conn.Value 事件挂接处未处理 NRE 死亡）
            Assert.Equal(1, pool.ReturnCount);
        }

        [Fact]
        public void Dispose_Twice_ReturnsConnectionOnce()
        {
            var pool = new FakePool();
            var so = CreateUnstarted(pool);
            so.Dispose();
            so.Dispose();
            so.Dispose();
            Assert.Equal(1, pool.ReturnCount);
        }

        [Fact]
        public void Start_Then_DisposeTwice_ReturnsConnectionOnce()
        {
            var pool = new FakePool();
            var so = CreateUnstarted(pool);
            so.Start();
            so.Dispose();
            so.Dispose();
            Thread.Sleep(200);
            Assert.Equal(1, pool.ReturnCount);
        }
    }
}
