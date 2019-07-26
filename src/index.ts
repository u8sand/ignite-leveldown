/// <reference types="easier-abstract-leveldown" />

import IgniteClient = require('apache-ignite-client')
import exposeLevelDOWN, { EasierLevelDOWNIteratorOpts, EasierLevelDOWNBatchOpts, EasierLevelDOWN } from 'easier-abstract-leveldown'
import { KeyVal } from 'easier-abstract-leveldown/dist/types'

const debug = require('debug')('ignite-leveldown')

const IgniteClientConfiguration = IgniteClient.IgniteClientConfiguration
const CacheConfiguration = IgniteClient.CacheConfiguration
const SqlFieldsQuery = IgniteClient.SqlFieldsQuery

interface IgniteDownOptions {
  location?: string
  uri: string
  cache: string
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

  async open() {
    this._igniteClient = new IgniteClient(this._onStateChange)

    await this._igniteClient.connect(new IgniteClientConfiguration(this._opts.uri))

    this._igniteCache = await this._igniteClient.getOrCreateCache(
      this._opts.cache,
      new CacheConfiguration().setSqlSchema('PUBLIC')
    )

    if (this._igniteCache === undefined) {
      throw new Error('IgniteCache could not be initialized')
    }

    await (await this._igniteCache.query(
      new SqlFieldsQuery(`
        CREATE TABLE IF NOT EXISTS kvstore (
          k CHAR(256),
          v CHAR(1024),
          PRIMARY KEY (k)
        ) WITH "template=partitioned, backups=1, affinityKey=k, CACHE_NAME=kvstore";
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

    const results = (await (await this._igniteCache.query(
      new SqlFieldsQuery(`
        update kvstore set v = ? where k = ?;
      `).setArgs(v, k)
    )).getAll())[0][0]

    // If nothing was updated, it doesn't yet exist
    if (results === 0) {
      await (await this._igniteCache.query(
        new SqlFieldsQuery(`
          insert into kvstore (k, v) values (?, ?);
        `).setArgs(k, v)
      )).getAll()
    }
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
        toDel.push({ key: opt.key })
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
          insert into kvstore (k, v)
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
            `where ${wheres.join('and')}`
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
