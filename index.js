const ras = require('random-access-storage')

const TYPE = {type: 'octet/stream'}
const requestFileSystem = window.requestFileSystem || window.webkitRequestFileSystem
const persistentStorage = navigator.persistentStorage || navigator.webkitPersistentStorage
const FileReader = window.FileReader
const Blob = window.Blob

createFile.DEFAULT_MAX_SIZE = Number.MAX_SAFE_INTEGER
createFile.requestQuota = requestQuota

module.exports = createFile

function requestQuota (n, force, cb) {
  if (typeof force === 'function') return requestQuota(n, true, force)
  persistentStorage.queryUsageAndQuota(function (used, quota) {
    if (quota && !force) return cb(null, quota)
    persistentStorage.requestQuota(n, function (quota) {
      cb(null, quota)
    }, cb)
  }, cb)
}

function createFile (name, opts) {
  if (!opts) opts = {}

  const maxSize = opts.maxSize || createFile.DEFAULT_MAX_SIZE
  const mutex = new Mutex()

  var fs = null
  var file = null
  var entry = null
  var toDestroy = null
  var readers = []
  var writers = []

  console.log('createFile:', name)

  return ras({read, write, open, stat, close, destroy})

  function read (req) {
    console.log('read request:', file.name + ' type: ' + req.type + ', offset: '+ req.offset + ', size: ' + req.size)
    const r = readers.pop() || new ReadRequest(readers, file, entry, mutex)
    r.run(req)
  }

  function write (req) {
    console.log('write request:', file.name + ' type: ' + req.type + ', offset: '+ req.offset + ', size: ' + req.size)
    const w = writers.pop() || new WriteRequest(writers, file, entry, mutex)
    w.run(req)
  }

  function close (req) {
    readers = writers = entry = file = fs = null
    req.callback(null)
  }

  function stat (req) {
    req.callback(null, file)
  }

  function destroy (req) {
    toDestroy.remove(ondone, onerror)

    function ondone () {
      toDestroy = null
      req.callback(null, null)
    }

    function onerror (err) {
      toDestroy = null
      req.callback(err, null)
    }
  }

  function open (req) {
    console.log('Open:', name)
    requestQuota(maxSize, false, function (err, granted) {
      if (err) return onerror(err)
      requestFileSystem(window.PERSISTENT, granted, function (res) {
        fs = res
        mkdirp(parentFolder(name), function () {
          fs.root.getFile(name, {create: true}, function (e) {
            entry = toDestroy = e
            entry.file(function (f) {
              console.log('File: ', f.name + ', lastModfied: ' + f.lastModified + ', size: ' + f.size)
              file = f
              req.callback(null)
            }, onerror)
          }, onerror)
        })
      }, onerror)
    })

    function mkdirp (name, ondone) {
      console.log('mkdirp:', name)
      if (!name) return ondone()
      fs.root.getDirectory(name, {create: true}, ondone, function () {
        mkdirp(parentFolder(name), function () {
          fs.root.getDirectory(name, {create: true}, ondone, ondone)
        })
      })
    }

    function onerror (err) {
      fs = file = entry = null
      req.callback(err)
    }
  }
}

function parentFolder (path) {
  const i = path.lastIndexOf('/')
  const j = path.lastIndexOf('\\')
  const p = path.slice(0, Math.max(0, i, j))
  return /^\w:$/.test(p) ? '' : p
}

function WriteRequest (pool, file, entry, mutex) {
  this.writer = null
  this.entry = entry
  this.file = file
  this.req = null
  this.pool = pool
  this.mutex = mutex
  this.locked = false
  this.truncating = false

  console.log('WriteRequest:', file.name)
}

WriteRequest.prototype.makeWriter = function () {
  const self = this
  console.log('makeWriter')
  this.entry.createWriter(function (writer) {
    self.writer = writer

    writer.onwriteend = function () {
      console.log('write request completed:', self.file.name)
      self.onwrite(null)
    }

    writer.onerror = function (err) {
      console.log('write request error:', self.file.name + ', ' + err)
      self.onwrite(err)
    }

    self.run(self.req)
  })
}

WriteRequest.prototype.onwrite = function (err) {
  const req = this.req
  this.req = null

  if (this.locked) {
    this.locked = false
    this.mutex.release()
  }

  if (this.truncating) {
    this.truncating = false
    if (!err) return this.run(req)
  }

  this.pool.push(this)
  req.callback(err, null)
}

WriteRequest.prototype.truncate = function () {
  this.truncating = true
  this.writer.truncate(this.req.offset)
}

WriteRequest.prototype.lock = function () {
  if (this.locked) return true
  this.locked = this.mutex.lock(this)
  return this.locked
}

WriteRequest.prototype.run = function (req) {
  this.req = req
  if (!this.writer || this.writer.length !== this.file.size) return this.makeWriter()

  const end = req.offset + req.size
  if (end > this.file.size && !this.lock()) return

  if (req.offset > this.writer.length) {
    if (req.offset > this.file.size) return this.truncate()
    return this.makeWriter()
  }

  console.log('WriteRequest run:', this.file.name + ', offset: ' + req.offset + ', size:' + req.size)
  this.writer.seek(req.offset)
  this.writer.write(new Blob([req.data], TYPE))
}

function Mutex () {
  this.queued = null
}

Mutex.prototype.release = function () {
  const queued = this.queued
  this.queued = null
  for (var i = 0; i < queued.length; i++) {
    queued[i].run(queued[i].req)
  }
}

Mutex.prototype.lock = function (req) {
  if (this.queued) {
    this.queued.push(req)
    return false
  }
  this.queued = []
  return true
}

function ReadRequest (pool, file, entry, mutex) {
  this.reader = new FileReader()
  this.file = file
  this.req = null
  this.pool = pool
  this.retry = true
  this.mutex = mutex
  this.locked = false

  const self = this

  console.log('ReadRequest:', file.name)

  this.reader.onerror = function () {
    console.log('ReadRequest error:', file.name + ', ' + this.error)
    self.onread(this.error, null)
  }

  this.reader.onload = function () {
    console.log('ReadRequest completed:', file.name)
    const buf = Buffer.from(this.result)
    self.onread(null, buf)
  }
}

ReadRequest.prototype.lock = function () {
  if (this.locked) return true
  this.locked = this.mutex.lock(this)
  return this.locked
}

ReadRequest.prototype.onread = function (err, buf) {
  const req = this.req

  if (err && this.retry) {
    this.retry = false
    console.log('retrying read')
    if (this.lock(this)) this.run(req)
    return
  }

  this.req = null
  console.log('readers push')
  this.pool.push(this)
  this.retry = true

  if (this.locked) {
    this.locked = false
    this.mutex.release()
  }

  req.callback(err, buf)
}

ReadRequest.prototype.run = function (req) {
  const end = req.offset + req.size
  this.req = req
  console.log('ReadRequest run:', this.file.name + ', type: ' + req.type + ', offset: ' + req.offset + ', size: ' + req.size + ', lastModified: ' + this.file.lastModified + ', file size: ' + this.file.size)
  if (end > this.file.size) return this.onread(new Error('Could not satisfy length'), null)
  this.reader.readAsArrayBuffer(this.file.slice(req.offset, end))
}
