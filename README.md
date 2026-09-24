# threadpool — 固定线程数 + 有界优先级队列线程池（纯标准库）

面向批处理服务的线程池：控制并发度、明确背压、优雅停机。仅依赖 Python 3 标准库。

## 运行命令

```bash
python3 test_threadpool.py -v     # 并发与关闭测试（21 个用例）
python3 leak_check.py             # 泄漏检查（300 轮 建池/提交/关闭）
python3 leak_check.py --cycles 1000 --workers 8 --tasks 1000   # 更大压力
```

## 快速上手

```python
from threadpool import ThreadPool, QueueFull, PoolShutdown

with ThreadPool(max_workers=4, maxsize=1000) as pool:
    f = pool.submit(pow, 2, 10, priority=1)   # priority 数值小者先出队
    print(f.result(timeout=5))                # 1024
# 退出 with 即优雅关闭：停收新任务，在途任务收尾后线程退出
```

## 错误码

所有池异常继承 `PoolError`，携带 `.code`（`ErrorCode` 枚举）：

| 异常 | code | 触发场景 |
|---|---|---|
| `QueueFull` | `QUEUE_FULL` | 队列满且 `block=False`，或阻塞等待空位超时 |
| `PoolShutdown` | `SHUTTING_DOWN` | 关闭后提交；阻塞中的提交者在关闭时被唤醒也收到它 |
| `TaskCancelled` | `CANCELLED` | 对未开始的任务调用 `future.cancel()` 成功 |
| `TaskAborted` | `ABORTED` | `abort()` 丢弃的未开始任务（可明确识别） |
| `TaskTimeout` | `TIMEOUT` | `future.result(timeout=...)` 超时 |

## 策略说明

### 背压
- 队列有界（`maxsize`，默认 1024，`None` 表示无界）。
- 满时两选一：`block=True`（默认）阻塞提交方，可用 `timeout` 限制；
  `block=False` 立即抛 `QueueFull(code=QUEUE_FULL)` —— 明确的拒绝信号。
- 优先级出队用堆实现：`(priority, 序号, task)`，数值小者优先，同级 FIFO。

### 关闭
- `shutdown(wait=True)`：置关闭标志 → 停收新任务 → 唤醒所有阻塞中的
  提交者（它们收到 `PoolShutdown`，不会挂死）→ 工作线程排空队列后退出 → join。
  幂等，可重复调用。
- `abort()`：立即中止。丢弃队列中所有未开始任务并置为 `ABORTED`
  （`future.aborted()` 为真，`result()` 抛 `TaskAborted`）；
  正在运行的任务无法被安全强杀（Python 线程语义），自然运行结束后线程退出。

### 不死锁的三个机制
1. **关闭期间提交**：`submit` 在锁内先检查关闭标志；因队列满而阻塞的提交者
   在 `shutdown()` 时被 `notify_all` 唤醒并重新检查标志，收到
   `PoolShutdown` 而非永久等待。
2. **池内任务互相等待**：工作线程调用本池某个 PENDING 任务的
   `future.result()` 时，会原子地"认领"该任务并**内联执行**（类似 .NET 的
   task inlining），把"线程耗尽型死锁"转化为普通的方法调用。
   已被其他 worker 运行的任务无法认领，退化为正常等待。
3. **超时兜底**：`result(timeout)` 超时抛 `TaskTimeout`，互等双方必有一方
   先超时退出，另一方随之解除阻塞（见 `test_mutual_wait_timeout_releases`）。

### 异常与资源释放
- 任务抛出的任何异常（含 `BaseException`）被捕获进 Future 并回传提交方，
  工作线程继续取下一个任务，不会被异常带走。
- 任务执行完即清空 `fn/args/kwargs` 引用；Future 完成后断开对 task 的引用；
  关闭后释放线程列表与 worker 注册表。守护线程 + join 保证解释器退出干净。

## 泄漏检查结果

`python3 leak_check.py --cycles 300`（300 轮 × 每轮 500 任务，含异常任务，
共 15 万任务、300 次建池/销毁）实测输出：

```
 cycle    RSS(MB)    gc_objs  threads
    20       14.5       8781        1
   100       14.9       8781        1
   200       14.9       8781        1
   300       15.0       8781        1

热身后 RSS 增长: +0.1 MB (预算 8.0 MB)
GC 对象数增长: +0
存活线程数: 1 (基线 1)
结果: PASS - 无持续增长迹象
```

GC 对象数零增长、线程数回到基线、RSS 平台期，三者共同证明线程与队列内存可释放。

## 测试覆盖（test_threadpool.py，21 例）

- 基本：结果/kwargs、优先级顺序（同级 FIFO）、完成回调、上下文管理器
- 背压：队列满拒绝（校验错误码）、阻塞等空位成功、阻塞超时拒绝
- 关闭：空队列关闭、重复关闭/中止幂等、优雅关闭排尽在途任务、
  关闭后提交被拒、阻塞提交者被关闭唤醒（不死锁）、abort 标记 ABORTED、
  关闭中取消排队任务
- 异常：异常回传且 worker 存活、池继续工作
- 不死锁：单线程池嵌套等待（内联执行）、双任务互等 + 内联、
  双任务互等 + 超时释放、result 超时错误码
