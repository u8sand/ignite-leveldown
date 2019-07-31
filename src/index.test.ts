import suite from 'easier-abstract-leveldown/dist/tests/index'
import ignitedown from '.'
import test = require('tape')

suite({
  test,
  factory: ignitedown({ uri: "127.0.0.1:10800", key_size: 256, value_size: 1024, }),
})
