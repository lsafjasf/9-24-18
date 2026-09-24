# threadpool — 固定线程数优先级线程池（仅标准库）

Python 3.8+，零第三方依赖（`threading` / `heapq` / `enum` / `time`）。

## 运行命令

```bash
python3 -m unittest test_threadpool -v   # 并发与关闭测试（20 个用例）
python3 leak_check.py                    # 长期运行内存稳定性检查（3 万个任务）
```

## 快速上手

```python
from threadpool import ThreadPool, PoolError, ErrorCode

with ThreadPool(workers=4, max_queue=100) as pool:
    h = pool.submit(my_fn, arg, priority=5)      # priority 越小越先出队
    try:
        print(h.result(timeout=10))
    except PoolError as e:
        print(e.code)                            # QUEUE_FULL / POOL_SHUTDOWN / ...

pool.shutdown(wait=True)          # 优雅关闭：排空队列、等在途任务
pool.shutdown(wait=True, abort=True)  # 立即中止：丢弃待执行任务（标记 ABORTED）
```

## 策略说明

### 背压（有界队列）
- 队列满时 `submit(..., block=False)` 立即抛 `PoolError(QUEUE_FULL)`（明确错误码）；
- `block=True`（默认）阻塞等待空位，可用 `timeout=` 限时，超时同样抛 `QUEUE_FULL`；
- 关闭开始时唤醒所有阻塞中的提交者并抛 `POOL_SHUTDOWN`，绝不挂死。

### 优雅关闭 / 立即中止
- `shutdown()`：停止接收新任务（之后提交抛 `POOL_SHUTDOWN`），工作线程排空队列、
  执行完在途任务后退出，`wait=True` 时 join 全部线程；
- `shutdown(abort=True)`：队列中所有 PENDING 任务原子地标记为 `ABORTED` 并唤醒其等待者
  （`result()` 抛 `PoolError(TASK_ABORTED)`，`handle.state` 可识别）；正在运行的任务
  允许跑完——CPython 无法安全地杀线程，这是有意的取舍；
- 关闭幂等：重复调用安全；先优雅后 abort 会升级为中止模式。

### 无死锁设计
- **关闭期间提交**：提交路径在锁内先检查 `_shutdown`，直接抛错，不进入等待；
  已阻塞在满队列上的提交者由 shutdown 的 `notify_all` 唤醒后复检标志并失败返回。
- **池内任务互等**：固定线程数下无界互等本质上不可解（所有 worker 都被等住）。
  本库的契约是：池内等待必须带超时 —— `result(timeout)` 超时抛
  `PoolError(TASK_TIMEOUT)` 并释放该 worker，等待方解旋后池恢复可用
  （见 `test_mutual_wait_with_timeout_unwinds`）。超时只释放等待者，不取消任务本身。
- **锁序**：池锁与每个 handle 的条件变量是叶锁关系，worker 持锁只做状态转移，
  用户回调（fn）永远在锁外执行，不存在持锁调用用户代码的路径。

### 异常与线程存活
- 任务抛出的异常被 worker 捕获并存入 handle，`result()` 向提交方重新抛出，
  `exception()` 可取回；worker 线程本身继续服务后续任务
  （见 `test_exception_propagates_and_worker_survives`）。

### 内存释放
- 队列项弹出后即失去引用；任务执行完立即释放 fn/args/kwargs 负载；
  handle 不持有任务负载，提交方丢弃后由 GC 回收（泄漏检查用 weakref 验证）；
- `join()` 完成后清空线程对象列表，worker 线程全部退出。

## 测试覆盖（test_threadpool.py）

| 场景 | 用例 |
| --- | --- |
| 基本结果 / 并发 | `test_result_and_parallelism` |
| 优先级出队（同级 FIFO） | `test_priority_order` |
| 异常回传、worker 存活 | `test_exception_propagates_and_worker_survives` |
| 队列满拒绝 / 阻塞成功 / 阻塞超时 | `Backpressure` 三个用例 |
| 满队列阻塞提交者被 shutdown 唤醒 | `test_blocked_submitter_woken_by_shutdown` |
| 空队列关闭 | `test_empty_queue_shutdown` |
| 重复关闭 / 升级为 abort | `test_repeated_shutdown_is_safe`、`test_shutdown_escalates_to_abort` |
| 优雅关闭排尽在途任务 | `test_graceful_shutdown_drains_inflight` |
| 关闭后 / 关闭中提交被拒 | `test_submit_after_shutdown_rejected`、`test_submit_during_shutdown_rejected` |
| abort 任务可识别 | `test_abort_marks_pending_tasks_identifiable` |
| 关闭中取消任务 | `test_cancel_during_shutdown` |
| 取消运行中 / 已完成任务失败 | `test_cancel_running_task_fails` |
| 池内互等超时解旋、池恢复 | `test_mutual_wait_with_timeout_unwinds` |
| 池内依赖链正常完成 | `test_dependent_chain_completes` |
| 线程退出与对象释放 | `test_workers_exit_and_release` |

## 泄漏检查结果（leak_check.py，60 轮 × 500 任务 = 3 万任务，含失败/取消/abort 路径）

```
baseline : rss=33904 KiB, gc_objects=7747, threads=5
round  60: rss=33904 KiB, gc_objects=7848, queue=0, threads=5
final    : rss=33904 KiB, gc_objects=7755, threads=1
handles  : 120 sampled, 0 still alive
  [PASS] rss growth: 0 KiB < 8192 KiB
  [PASS] gc object growth: 8 < 5000
  [PASS] thread count: 1 <= 5 (workers exited)
  [PASS] queue drained: queue_size=0
  [PASS] handles released: 0 alive
LEAK CHECK: PASS
```
