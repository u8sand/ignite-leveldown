import suite from 'easier-abstract-leveldown/dist/tests/index'
import ignitedown from '.'
import test = require('tape')
import * as uuidv4 from 'uuid/v4'

suite({
  test,
  factory: (...args) =>
    ignitedown({ location: `ignite://127.0.0.1:10800/cache-${uuidv4()}`, key_size: 256, value_size: 1024, })(...args),
})
