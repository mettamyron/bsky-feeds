import pg from 'pg'
import dotenv from 'dotenv'
import { InvalidRequestError } from '@atproto/xrpc-server'

dotenv.config()

export class DbSingleton {
  private pool: pg.Pool

  constructor(connectionString: string) {
    this.pool = new pg.Pool({ connectionString })
    this.init()
  }

  async init() {
    const client = await this.pool.connect()
    try {
      await this.createTables(client)
      await this.createIndexes(client)
    } finally {
      client.release()
    }
  }

  private async createTables(client: pg.PoolClient) {
    await client.query(`
      CREATE TABLE IF NOT EXISTS post (
        id SERIAL PRIMARY KEY,
        uri TEXT UNIQUE NOT NULL,
        cid TEXT NOT NULL,
        author TEXT NOT NULL,
        text TEXT NOT NULL,
        reply_parent TEXT,
        reply_root TEXT,
        indexed_at BIGINT NOT NULL,
        has_image BOOLEAN NOT NULL,
        embed JSONB,
        algo_tags TEXT[],
        labels TEXT[],
        sort_weight FLOAT
      );

      CREATE TABLE IF NOT EXISTS sub_state (
        id SERIAL PRIMARY KEY,
        service TEXT UNIQUE NOT NULL,
        cursor BIGINT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS list_members (
        id SERIAL PRIMARY KEY,
        did TEXT UNIQUE NOT NULL
      );

      CREATE TABLE IF NOT EXISTS collection (
        id SERIAL PRIMARY KEY,
        key TEXT UNIQUE NOT NULL,
        value JSONB NOT NULL
      );
    `)
  }

  private async createIndexes(client: pg.PoolClient) {
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_post_uri ON post(uri);
      CREATE INDEX IF NOT EXISTS idx_post_indexed_at_cid ON post(indexed_at DESC, cid DESC);
      CREATE INDEX IF NOT EXISTS idx_post_algo_tags ON post USING GIN(algo_tags);
      CREATE INDEX IF NOT EXISTS idx_post_author ON post(author);
      CREATE INDEX IF NOT EXISTS idx_post_labels ON post USING GIN(labels);
      CREATE INDEX IF NOT EXISTS idx_post_embed_images ON post((embed->'images'));
      CREATE INDEX IF NOT EXISTS idx_post_embed_media ON post((embed->'media'));
      CREATE INDEX IF NOT EXISTS idx_post_sort_weight ON post(sort_weight DESC);
      CREATE INDEX IF NOT EXISTS idx_collection_key ON collection(key);
    `)
  }

  async deleteAll(table: string) {
    await this.pool.query(`DELETE FROM ${table}`)
  }

  async deleteManyURI(table: string, uris: string[]) {
    await this.pool.query(`DELETE FROM ${table} WHERE uri = ANY($1)`, [uris])
  }

  async deleteManyDID(dids: string[]) {
    await this.pool.query(`DELETE FROM list_members WHERE did = ANY($1)`, [
      dids,
    ])
  }

  async replaceOneURI(table: string, uri: string, data: any) {
    const columns = Object.keys(data)
    const values = Object.values(data)
    const placeholders = values.map((_, i) => `$${i + 2}`)

    await this.pool.query(
      `
      INSERT INTO ${table} (uri, ${columns.join(', ')})
      VALUES ($1, ${placeholders.join(', ')})
      ON CONFLICT (uri) DO UPDATE SET
      ${columns.map((col, i) => `${col} = $${i + 2}`).join(', ')}
    `,
      [uri, ...values],
    )
  }

  async replaceManyURI(table: string, data: any[]) {
    const client = await this.pool.connect()
    try {
      await client.query('BEGIN')
      for (const item of data) {
        await this.replaceOneURI(table, item.uri, item)
      }
      await client.query('COMMIT')
    } catch (e) {
      await client.query('ROLLBACK')
      throw e
    } finally {
      client.release()
    }
  }

  async replaceOneDID(table: string, did: string, data: any) {
    const columns = Object.keys(data)
    const values = Object.values(data)
    const placeholders = values.map((_, i) => `$${i + 2}`)

    await this.pool.query(
      `
      INSERT INTO ${table} (did, ${columns.join(', ')})
      VALUES ($1, ${placeholders.join(', ')})
      ON CONFLICT (did) DO UPDATE SET
      ${columns.map((col, i) => `${col} = $${i + 2}`).join(', ')}
    `,
      [did, ...values],
    )
  }

  async updateSubStateCursor(service: string, cursor: string | number) {
    const cursorValue = typeof cursor === 'number' ? cursor.toString() : cursor
    await this.pool.query(
      `
      INSERT INTO sub_state (service, cursor)
      VALUES ($1, $2)
      ON CONFLICT (service) DO UPDATE SET cursor = $2
    `,
      [service, cursorValue],
    )
  }

  async getSubStateCursor(service: string) {
    const result = await this.pool.query(
      'SELECT * FROM sub_state WHERE service = $1',
      [service],
    )
    return result.rows[0]
  }

  async getLatestPostsForTag(
    tag: string,
    limit = 50,
    cursor: string | undefined = undefined,
    imagesOnly: boolean = false,
    nsfwOnly: boolean = false,
    excludeNSFW: boolean = false,
    authors: string[] = [],
  ) {
    let query = `
      SELECT * FROM post
      WHERE $1 = ANY(algo_tags)
    `
    const params: any[] = [tag]
    let paramIndex = 2

    if (nsfwOnly) {
      query += ` AND labels && $${paramIndex}`
      params.push(['porn', 'nudity', 'sexual', 'underwear'])
      paramIndex++
    }

    if (excludeNSFW) {
      query += ` AND NOT (labels && $${paramIndex})`
      params.push(['porn', 'nudity', 'sexual', 'underwear'])
      paramIndex++
    }

    if (authors.length > 0) {
      query += ` AND author = ANY($${paramIndex})`
      params.push(authors)
      paramIndex++
    }

    if (cursor !== undefined) {
      const [indexedAt, cid] = cursor.split('::')
      if (!indexedAt || !cid) {
        throw new InvalidRequestError('malformed cursor')
      }
      query += ` AND (indexed_at < $${paramIndex} OR (indexed_at = $${paramIndex} AND cid < $${
        paramIndex + 1
      }))`
      params.push(parseInt(indexedAt, 10), cid)
      paramIndex += 2
    }

    query += ` ORDER BY indexed_at DESC, cid DESC LIMIT $${paramIndex}`
    params.push(limit)

    const result = await this.pool.query(query, params)
    return result.rows
  }

  async getTaggedPostsBetween(tag: string, start: number, end: number) {
    const larger = Math.max(start, end)
    const smaller = Math.min(start, end)

    const result = await this.pool.query(
      `
      SELECT * FROM post
      WHERE indexed_at > $1 AND indexed_at < $2 AND $3 = ANY(algo_tags)
      ORDER BY indexed_at DESC, cid DESC
    `,
      [smaller, larger, tag],
    )

    return result.rows
  }

  async getDistinctFromCollection(table: string, field: string) {
    const result = await this.pool.query(
      `SELECT DISTINCT ${field} FROM ${table}`,
    )
    return result.rows.map((row) => row[field])
  }

  async removeTagFromPostsForAuthor(tag: string, authors: string[]) {
    await this.pool.query(
      `
      UPDATE post
      SET algo_tags = array_remove(algo_tags, $1)
      WHERE author = ANY($2)
    `,
      [tag, authors],
    )

    await this.deleteUntaggedPosts()
  }

  async removeTagFromOldPosts(tag: string, indexedAt: number) {
    await this.pool.query(
      `
      UPDATE post
      SET algo_tags = array_remove(algo_tags, $1)
      WHERE indexed_at < $2
    `,
      [tag, indexedAt],
    )

    await this.deleteUntaggedPosts()
  }

  async getUnlabelledPostsWithMedia(limit = 100, lagTime = 60 * 1000) {
    const result = await this.pool.query(
      `
      SELECT * FROM post
      WHERE (embed->'images' IS NOT NULL OR embed->'media' IS NOT NULL)
      AND labels IS NULL
      AND indexed_at < $1
      ORDER BY indexed_at DESC, cid DESC
      LIMIT $2
    `,
      [new Date().getTime() - lagTime, limit],
    )

    return result.rows
  }

  async updateLabelsForURIs(postEntries: { uri: string; labels: string[] }[]) {
    const client = await this.pool.connect()
    try {
      await client.query('BEGIN')
      for (const entry of postEntries) {
        await client.query(
          `
          UPDATE post
          SET labels = $1
          WHERE uri = $2
        `,
          [entry.labels, entry.uri],
        )
      }
      await client.query('COMMIT')
    } catch (e) {
      await client.query('ROLLBACK')
      throw e
    } finally {
      client.release()
    }
  }

  async deleteUntaggedPosts() {
    await this.pool.query(
      `DELETE FROM post WHERE array_length(algo_tags, 1) IS NULL`,
    )
  }

  async deleteSqueakyCleanPosts() {
    const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000)

    await this.pool.query(
      `
      DELETE FROM post
      WHERE 'squeaky-clean' = ANY(algo_tags)
      AND NOT ('mutuals-ad' = ANY(algo_tags))
      AND created_at < $1
    `,
      [fiveMinutesAgo],
    )
  }

  async getPostForURI(uri: string) {
    const result = await this.pool.query('SELECT * FROM post WHERE uri = $1', [
      uri,
    ])
    return result.rows[0] || null
  }

  async aggregatePostsByRepliesToCollection(
    collection: string,
    tag: string,
    threshold: number,
    out: string,
    limit: number = 100,
  ) {
    const result = await this.pool.query(
      `
      SELECT reply_parent, COUNT(*) as reply_count
      FROM post
      WHERE reply_parent IS NOT NULL
      GROUP BY reply_parent
      ORDER BY reply_count DESC
      LIMIT $1
    `,
      [limit],
    )

    return result.rows
  }

  async getPostBySortWeight(collection: string, limit: number = 50, cursor?: string) {
    let query = `
      SELECT * FROM post
      WHERE sort_weight IS NOT NULL
    `
    const params: any[] = []
    let paramIndex = 1

    if (cursor) {
      const [sortWeight, cid] = cursor.split('::')
      if (!sortWeight || !cid) {
        throw new InvalidRequestError('malformed cursor')
      }
      query += ` AND (sort_weight < $${paramIndex} OR (sort_weight = $${paramIndex} AND cid < $${
        paramIndex + 1
      }))`
      params.push(parseFloat(sortWeight), cid)
      paramIndex += 2
    }

    query += ` ORDER BY sort_weight DESC, cid DESC LIMIT $${paramIndex}`
    params.push(limit)

    const result = await this.pool.query(query, params)
    return result.rows
  }

  async getCollection(key: string) {
    const result = await this.pool.query(
      'SELECT value FROM collection WHERE key = $1',
      [key],
    )
    return result.rows[0]?.value || null
  }

  async insertOrReplaceRecord(key: any, value: any, collection: string) {
    await this.pool.query(`
      INSERT INTO collection (key, value)
      VALUES ($1, $2)
      ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
  `, [key, JSON.stringify(value)]);
  }

  async getRecentAuthorsForTag(
    tag: string,
    limit: number = 50,
    cursor?: string,
  ) {
    let query = `
      SELECT DISTINCT ON (author) author, indexed_at, cid
      FROM post
      WHERE $1 = ANY(algo_tags)
    `
    const params: any[] = [tag]
    let paramIndex = 2

    if (cursor) {
      const [indexedAt, cid] = cursor.split('::')
      if (!indexedAt || !cid) {
        throw new InvalidRequestError('malformed cursor')
      }
      query += ` AND (indexed_at < $${paramIndex} OR (indexed_at = $${paramIndex} AND cid < $${
        paramIndex + 1
      }))`
      params.push(parseInt(indexedAt, 10), cid)
      paramIndex += 2
    }

    query += ` ORDER BY author, indexed_at DESC, cid DESC LIMIT $${paramIndex}`
    params.push(limit)

    const result = await this.pool.query(query, params)
    return result.rows
  }
}

const dbClient = new DbSingleton(
  process.env.FEEDGEN_POSTGRES_CONNECTION_STRING || '',
)

export default dbClient
