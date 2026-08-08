-- =============================================================================
-- MEDINA SCHEDULER  (v1.5)
-- A tiny cooperative task engine for OpenComputers.
--
-- The whole idea: instead of one big loop that freezes whenever it has to wait,
-- you spawn small "tasks" (coroutines) that yield while waiting. The scheduler
-- resumes whichever tasks are ready each tick, then hands control straight back
-- to your main loop so the UI keeps drawing and telemetry keeps flowing.
--
-- Three things to learn, and that's the entire API:
--
--   sched.spawn(fn[, name])      -- run fn as a task; it can sleep/await freely
--   sched.tick()                 -- call once per main-loop pass; advances tasks
--   sched.lock(name)             -- a fairness lock for a shared resource
--
-- Inside a task you may call (NOT from the main loop — only inside a task):
--
--   sched.sleep(seconds)         -- pause this task, let others run
--   sched.await(fn[, timeout])   -- pause until fn() is truthy (or timeout)
--   lock:acquire()  / lock:release()
--   lock:with(fn)                -- acquire, run fn(), always release
--
-- ONE clock governs everything: computer.uptime() (real seconds since boot).
-- No mixing of os.time() world-ticks with real-time sleeps — every wait in the
-- system is measured the same way.
--
-- To add a new background feature later, you write a function and spawn it.
-- You never touch this file. That's the point.
-- =============================================================================

local computer = require("computer")

local scheduler = {}

-- Live tasks. Each entry: { co, name, wake (uptime to resume at), cond (await fn),
-- deadline (await timeout), dead (bool) }
local ready_tasks = {}   -- tasks that can run immediately (no sleep/await)
local waiting_tasks = {} -- tasks currently sleeping or awaiting a condition
local nextId = 1

local function now() return computer.uptime() end

-- ---------------------------------------------------------------------------
-- YIELD PROTOCOL
-- A task yields a small table describing why it paused. tick() reads it and
-- decides when to resume. Tasks never see this table — they use sleep/await.
-- ---------------------------------------------------------------------------

-- Pause the current task for `seconds`.
function scheduler.sleep(seconds)
  coroutine.yield({ kind = "sleep", seconds = seconds or 0 })
end

-- Pause the current task until `condition()` returns truthy.
-- Optional `timeout` (seconds): if it elapses first, await() returns false.
-- Optional `interval` (seconds): minimum gap between condition checks. Defaults
--   to 0.1s so we don't hammer hardware calls (db.get, transposer reads) on
--   every single main-loop pass. The condition is checked once immediately, then
--   at most once per `interval` thereafter.
-- Returns true if the condition was met, false if it timed out.
function scheduler.await(condition, timeout, interval)
  local ok = coroutine.yield({
    kind     = "await",
    cond     = condition,
    timeout  = timeout,
    interval = interval,
  })
  return ok
end

-- ---------------------------------------------------------------------------
-- LOCKS
-- A fair (FIFO) lock so concurrent tasks can take turns on a shared component.
-- We default to NOT using one for the loader, but it's here for when a real
-- shared resource needs serializing — and it reads honestly in the code.
-- ---------------------------------------------------------------------------

function scheduler.lock(name)
  local lock = { name = name, held = false, waiters = 0 }

  function lock:acquire()
    if not self.held then
      self.held = true
      return true
    end
    -- Wait in FIFO order.
    self.waiters = self.waiters + 1
    local granted = false
    coroutine.yield({ kind = "lock_wait", lock = lock, self_ref = self })
    granted = true
    return granted
  end

  function lock:release()
    if self.held then
      self.held = false
      -- Wake the next waiter, if any.
      if self.waiters > 0 then
        self.waiters = self.waiters - 1
        -- Find the next waiter and resume it.
        for _, task in ipairs(waiting_tasks) do
          if task.lockWait and task.lockWait == lock then
            coroutine.resume(task.co, true)
            return
          end
        end
      end
    end
  end

  function lock:with(fn)
    self:acquire()
    local ok, result = pcall(fn)
    self:release()
    if not ok then error(result) end
    return result
  end

  return lock
end

-- ---------------------------------------------------------------------------
-- SPAWN / TICK
-- ---------------------------------------------------------------------------

-- Start `fn` as a task. `name` is optional, used in error reporting.
-- Returns a handle you can poll with handle:done().
function scheduler.spawn(fn, name)
  local task = {
    id       = nextId,
    co       = coroutine.create(fn),
    name     = name or ("task#" .. nextId),
    wake     = 0,        -- resume when now() >= wake (0 = asap)
    cond     = nil,      -- await predicate, if any
    deadline = nil,      -- await timeout absolute uptime
    dead     = false,
    lockWait = nil,      -- lock this task is waiting on
    _inWaiting = false,  -- internal: tracks which bucket the task is in
  }
  nextId = nextId + 1
  ready_tasks[#ready_tasks + 1] = task
  return {
    done = function() return task.dead end,
    name = task.name,
  }
end

-- True if any task is still alive (useful for "wait until all loads finish").
function scheduler.busy()
  for _, t in ipairs(ready_tasks) do
    if not t.dead then return true end
  end
  for _, t in ipairs(waiting_tasks) do
    if not t.dead then return true end
  end
  return false
end

-- How many tasks are currently alive.
function scheduler.count()
  local n = 0
  for _, t in ipairs(ready_tasks) do if not t.dead then n = n + 1 end end
  for _, t in ipairs(waiting_tasks) do if not t.dead then n = n + 1 end end
  return n
end

-- Optional error hook: scheduler.onError = function(name, err) ... end
scheduler.onError = nil

-- Resume one task if it's ready. Returns nothing; mutates task state.
local function step(task)
  if task.dead then return end

  local t = now()

  -- Still sleeping? (task.wake is set when task yields with sleep)
  if task.wake and t < task.wake then 
    -- Move to waiting bucket (if not already there)
    if task._inWaiting then return end  -- already in waiting bucket, skip
    task._inWaiting = true
    waiting_tasks[#waiting_tasks + 1] = task
    return 
  end

  -- Awaiting a condition?
  local resumeValue = nil
  if task.cond then
    -- Throttle: only re-check once per interval, not every main-loop pass.
    if task.nextCheck and t < task.nextCheck then
      return
    end
    task.nextCheck = t + (task.interval or 0.1)

    local met = false
    local ok, res = pcall(task.cond)
    if ok and res then met = true end

    local timedOut = task.deadline and t >= task.deadline
    if not met and not timedOut then
      return  -- keep waiting
    end
    -- Condition met (true) or timed out (false) — tell await() which.
    resumeValue = met
    task.cond = nil
    task.deadline = nil
    task.nextCheck = nil
  end

  -- Resume the coroutine.
  local ok, yielded = coroutine.resume(task.co, resumeValue)

  if not ok then
    -- Task crashed.
    task.dead = true
    if scheduler.onError then
      pcall(scheduler.onError, task.name, yielded)
    end
    return
  end

  if coroutine.status(task.co) == "dead" then
    task.dead = true
    return
  end

  -- Task yielded a wait request — record it and move to waiting bucket.
  if type(yielded) == "table" then
    if yielded.kind == "sleep" then
      task.wake = now() + (yielded.seconds or 0)
      task.cond = nil
      task.deadline = nil
      -- Move to waiting bucket
      task._inWaiting = true
      waiting_tasks[#waiting_tasks + 1] = task
    elseif yielded.kind == "await" then
      task.wake = 0
      task.cond = yielded.cond
      task.deadline = yielded.timeout and (now() + yielded.timeout) or nil
      task.interval = yielded.interval or 0.1
      task.nextCheck = nil  -- check immediately on next step, then throttle
      -- Move to waiting bucket (will be moved back to ready in next tick if wake time passed)
      task._inWaiting = true
      waiting_tasks[#waiting_tasks + 1] = task
    end
  else
    -- Bare yield with no request: resume next tick (stay in ready bucket)
    task.wake = 0
    task.cond = nil
    task.deadline = nil
  end
end

-- Call once per main-loop pass. Advances every ready task by one step, then
-- returns immediately. Never blocks.
function scheduler.tick()
  -- Move tasks from waiting to ready if their wake time has passed.
  local still_waiting = {}
  for _, t in ipairs(waiting_tasks) do
    if not t.dead then
      if not t.wake or now() >= t.wake then
        ready_tasks[#ready_tasks + 1] = t
        t._inWaiting = false
      else
        still_waiting[#still_waiting + 1] = t
      end
    end
  end
  waiting_tasks = still_waiting

  -- Process all ready tasks (only iterate over this bucket).
  local n = #ready_tasks
  for i = 1, n do
    local task = ready_tasks[i]
    if task then step(task) end  -- step may move task to waiting_tasks
  end

  -- Compact: drop dead tasks so the lists don't grow forever.
  local live_ready = {}
  for _, t in ipairs(ready_tasks) do
    if not t.dead then live_ready[#live_ready + 1] = t end
  end
  ready_tasks = live_ready

  local live_waiting = {}
  for _, t in ipairs(waiting_tasks) do
    if not t.dead then live_waiting[#live_waiting + 1] = t end
  end
  waiting_tasks = live_waiting
end

return scheduler
