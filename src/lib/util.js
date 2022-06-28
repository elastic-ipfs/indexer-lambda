'use strict'

const config = require('../config')

/* c8 ignore next 3 */
function now() {
  return config.now || new Date().toISOString()
}

/**
 * execute tasks in parallel with concurrency
 * starts on the first "add"
 * run "concurrency" tasks at time
 * calls "onTaskComplete" after each task, passing the task's output
 * stops at first error
 * does not throw error, return it on "done"
 */
function queuedTasks({ concurrency = 1, onTaskComplete = noop } = {}) {
  const _queue = []
  let running = 0
  let error

  let _resolve

  function _done() {
    if (error) {
      _resolve && _resolve({ error })
      return
    }
    if (_queue.length > 0) {
      run()
      return
    }
    if (running === 0) {
      _resolve && _resolve({ error })
    }
  }

  function add(f) {
    _queue.push(f)
    run()
  }

  async function run() {
    if (running >= concurrency) {
      return
    }
    const f = _queue.shift()
    if (!f) {
      _done()
      return
    }
    running++
    try {
      const result = await f()
      onTaskComplete(result)
    } catch (err) {
      error = err
    } finally {
      running--
      _done()
    }
  }

  function done() {
    return new Promise(resolve => {
      _resolve = resolve
      _done()
    })
  }

  return { add, run, done }
}

function noop() {}

module.exports = {
  now,
  queuedTasks
}
