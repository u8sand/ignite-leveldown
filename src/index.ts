/// <reference types="easier-abstract-leveldown" />

import { KeyVal } from 'easier-abstract-leveldown/dist/types'
import { URL } from 'url'
import exposeLevelDOWN, { EasierLevelDOWNIteratorOpts, EasierLevelDOWNBatchOpts, EasierLevelDOWN } from 'easier-abstract-leveldown'
import IgniteClient = require('apache-ignite-client')

const debug = require('debug')('ignite-leveldown')

const IgniteClientConfiguration = IgniteClient.IgniteClientConfiguration
const CacheConfiguration = IgniteClient.CacheConfiguration
const SqlFieldsQuery = IgniteClient.SqlFieldsQuery

interface IgniteDownOptions {
  // The location string provided to the leveldown instance
  //  should be prefixed by the uri to access the ignite cluster i.e. ignite://127.0.0.1:10800/cache_name
  location?: string
  // The maximum size for a key field
  key_size: number
  // The maximum size for a value field
  value_size: number
}

export class IgniteDown<K extends string, V extends {}> implements EasierLevelDOWN<K, V, IgniteDownOptions> {
  _opts: IgniteDownOptions
  _igniteClient: IgniteClient
  _igniteCache: any

  constructor(opts: IgniteDownOptions) {
    this._opts = opts
  }

  _onStateChange = (state, reason) => {
    if (state === IgniteClient.STATE.CONNECTED) {
      debug('Client is started')
    } else if (state === IgniteClient.STATE.DISCONNECTED) {
      debug('Client is stopped')
      if (reason) {
        debug(reason)
      }
    }
  }

  async open(opts: IgniteDownOptions) {
    // Add location to the given default location i.e. operates like a prefix
    if (opts.location !== undefined) {
      this._opts.location += opts.location
    }

    const url = new URL(this._opts.location)
    const uri = `${url.host}`
    const cache = `${url.pathname.slice(1)}`

    this._igniteClient = new IgniteClient(this._onStateChange)

    await this._igniteClient.connect(new IgniteClientConfiguration(uri))

    this._igniteCache = await this._igniteClient.getOrCreateCache(
      cache,
      new CacheConfiguration().setSqlSchema('PUBLIC')
    )

    if (this._igniteCache === undefined) {
      throw new Error('IgniteCache could not be initialized')
    }

    await (await this._igniteCache.query(
      new SqlFieldsQuery(`
        CREATE TABLE IF NOT EXISTS kvstore (
          k CHAR(${this._opts.key_size}),
          v CHAR(${this._opts.value_size}),
          PRIMARY KEY (k)
        ) WITH "template=partitioned, backups=1, affinityKey=k, CACHE_NAME=${cache}_kvstore";
      `)
    )).getAll()

    await (await this._igniteCache.query(
      new SqlFieldsQuery(`
        CREATE INDEX IF NOT EXISTS kvstore_k ON kvstore (k)
      `)
    )).getAll()
  }

  async close() {
    this._igniteClient.disconnect()
  }

  async get(k: K) {
    if (this._igniteCache === undefined) {
      throw new Error('IgniteCache was not initialized')
    }

    const value = (await (await this._igniteCache.query(
      new SqlFieldsQuery(`
        select v
        from kvstore
        where k = ?;
      `).setArgs(k)
    )).getAll())

    if (value.length === 0) {
      throw new Error('NotFound')
    }

    return value[0][0]
  }

  async put(k: K, v: V) {
    if (this._igniteCache === undefined) {
      throw new Error('IgniteCache was not initialized')
    }

    (await (await this._igniteCache.query(
      new SqlFieldsQuery(`
        merge into kvstore set v = ? where k = ?;
      `).setArgs(v, k)
    )).getAll())
  }

  async del(k: K) {
    if (this._igniteCache === undefined) {
      throw new Error('IgniteCache was not initialized')
    }

    await (await this._igniteCache.query(
      new SqlFieldsQuery(`
        delete from kvstore
        where k = ?;
      `).setArgs(k)
    )).getAll()
  }

  async batch(opts: EasierLevelDOWNBatchOpts<K, V>) {
    if (this._igniteCache === undefined) {
      throw new Error('IgniteCache was not initialized')
    }

    const toDel = []
    const toPut = []

    for (const opt of opts) {
      if (opt.type == 'put') {
        toPut.push({ key: opt.key, value: opt.value })
      } else if (opt.type === 'del') {
        toDel.push({ key: opt.key })
      }
    }
    if (toDel) {
      await (await this._igniteCache.query(
        new SqlFieldsQuery(`
          delete from kvstore
          where k in (${toDel.map(() => '?').join(',')});
        `).setArgs(...toDel.map(({ key }) => key))
      )).getAll()
    }
    if (toPut) {
      await (await this._igniteCache.query(
        new SqlFieldsQuery(`
          merge into kvstore (k, v)
          values ${toPut.map(() => '(?, ?)').join(',')};
        `).setArgs(...toPut.reduce(
          (args, { key, value }) => [...args, key, value], []
        ))
      )).getAll()
    }
  }

  async *iterator(opts: EasierLevelDOWNIteratorOpts<K, V>): AsyncIterableIterator<KeyVal<K, V>> {
    if (this._igniteCache === undefined) {
      throw new Error('IgniteCache was not initialized')
    }

    const wheres = []
    const args = []

    if (opts.lt !== undefined) {
      wheres.push('k < ?')
      args.push(opts.lt)
    } else if (opts.lte !== undefined) {
      wheres.push('k <= ?')
      args.push(opts.lte)
    }

    if (opts.gt !== undefined) {
      wheres.push('k > ?')
      args.push(opts.gt)
    } else if (opts.gte !== undefined) {
      wheres.push('k >= ?')
      args.push(opts.gte)
    }

    const cursor = await this._igniteCache.query(
      new SqlFieldsQuery(`
        select k, v
        from kvstore
        ${wheres ? (
            `where ${wheres.join(' and ')}`
          ) : ''}
        order by k ${
          opts.reverse === true ? (
            `desc`
          ) : (
            `asc`
          )
        };
      `).setArgs(...args)
    )

    for (const [key, value] of cursor) {
      yield { key, value }
    }
  }
}

export default (opts: IgniteDownOptions) => exposeLevelDOWN(() => new IgniteDown(opts))
