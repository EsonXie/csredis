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
    /// 注意：连接值为 null，订阅线程若启动会在入口 null 守卫处（Subscribe 方法解包后）安静返回，
    /// 不进入 while 循环与 Ping、不会进入 3s 重试循环；
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
            Thread.Sleep(200); // 给订阅线程退出时间（线程于入口 null 守卫处退出，不会求值 IsUnsubscribed）
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

        // PSubscribeObject：连接值为 null，PSubscribe 线程若启动会在入口 null 守卫处（PSubscribe 方法解包后）安静返回，
        // 不进入 while 循环与 Ping、不会进入 3s 重试循环；
        // 构造函数不启动线程，须显式 Start()，Dispose 幂等（CAS），与 SubscribeObject 同一模式。
        private static CSRedisClient.PSubscribeObject CreatePUnstarted(FakePool pool)
        {
            var conn = CSRedis.Internal.ObjectPool.Object<RedisClient>.InitWith(pool, 1, null);
            return new CSRedisClient.PSubscribeObject(
                redis: null,
                channels: new[] { "chan1*" },
                redisConnections: new[] { conn },
                onPMessage: _ => { },
                logger: new LoggerFactory().CreateLogger<CSRedisClient.PSubscribeObject>());
        }

        [Fact]
        public void PSubscribeObject_DisposeTwice_ReturnsConnectionOnce()
        {
            var pool = new FakePool();
            var so = CreatePUnstarted(pool);
            so.Dispose();
            so.Dispose();
            Assert.Equal(1, pool.ReturnCount);
        }

        [Fact]
        public void PSubscribeObject_ConstructWithoutStart_DoesNotReturnConnection()
        {
            var pool = new FakePool();
            var so = CreatePUnstarted(pool);
            Thread.Sleep(200);
            Assert.Equal(0, pool.ReturnCount);
            Assert.False(so.IsPUnsubscribed);
        }

        [Fact]
        public void PSubscribeObject_Start_Then_DisposeTwice_ReturnsConnectionOnce()
        {
            var pool = new FakePool();
            var so = CreatePUnstarted(pool);
            so.Start();
            so.Dispose();
            so.Dispose();
            Thread.Sleep(200); // 线程于入口 null 守卫处安静退出（不进入循环与 Ping）
            Assert.True(so.IsPUnsubscribed);
            Assert.Equal(1, pool.ReturnCount);
        }

        // CSRedisClient 订阅注册表：客户端 Dispose 必须先离线全部已跟踪订阅对象、再释放连接池。
        // new CSRedisClient("127.0.0.1:6379") 仅构建连接池对象，不建立网络连接（本 fork 需显式传入 LoggerFactory）。
        [Fact]
        public void ClientDispose_OfflinesAllTrackedSubscriptions()
        {
            var pool1 = new FakePool();
            var pool2 = new FakePool();
            using var client = new CSRedisClient("127.0.0.1:6379", new LoggerFactory());
            var so1 = CreateUnstarted(pool1);
            var so2 = CreateUnstarted(pool2);
            client.TrackSubscribeObject(so1);
            client.TrackSubscribeObject(so2);

            client.Dispose();

            Assert.True(so1.IsUnsubscribed);
            Assert.True(so2.IsUnsubscribed);
            Assert.Equal(1, pool1.ReturnCount);
            Assert.Equal(1, pool2.ReturnCount);
        }

        [Fact]
        public void ClientDispose_AfterManualDispose_DoesNotDoubleReturn()
        {
            var pool1 = new FakePool();
            using var client = new CSRedisClient("127.0.0.1:6379", new LoggerFactory());
            var so1 = CreateUnstarted(pool1);
            client.TrackSubscribeObject(so1);
            so1.Dispose();          // 先手动离线（已 Untrack + 已归还）
            client.Dispose();       // 注册表已无 so1，不得重复归还
            Assert.Equal(1, pool1.ReturnCount);
        }
    }
}
